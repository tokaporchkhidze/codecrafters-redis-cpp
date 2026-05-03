#include <array>
#include <cerrno>
#include <print>
#include <string_view>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

#include "redis_core/resp-decoder.h"
#include "tcp-connection.h"

#include "redis_core/resp-encoder.h"

using namespace redis_net;

TcpConnection::TcpConnection(EventLoop &event_loop,
                             int const fd,
                             CloseCallback on_close,
                             RedisExecutorPtr p_redis_executor) :
    event_loop_{event_loop},
    fd_{fd},
    p_redis_executor_(std::move(p_redis_executor)),
    on_close_{std::move(on_close)}
{
}

TcpConnection::~TcpConnection() noexcept { close_connection(); }

int TcpConnection::fd() const { return fd_; }

void TcpConnection::start()
{
  auto self{shared_from_this()};
  event_loop_.add(fd_,
                  EPOLLIN | EPOLLRDHUP,
                  EventLoop::Handler{
                          .on_read = [self] { self->handle_read(); },
                          .on_write = [self] { self->handle_write(); },
                          .on_close = [self] { self->handle_close(); },
                          .on_error = [self](std::error_code const error)
                          { self->handle_error(error); },
                  });
}

void TcpConnection::handle_read()
{
  std::array<uint8_t, 4096> buffer{};
  while (true) {
    auto const bytes_read{recv(fd_, buffer.data(), buffer.size(), 0)};
    if (bytes_read > 0) {
      // TODO: Need to implement better buffer handling.
      input_buffer_.insert(
              input_buffer_.end(), buffer.begin(), buffer.begin() + bytes_read);

      while (!input_buffer_.empty()) {
        std::string_view const request{
                reinterpret_cast<char const *>(input_buffer_.data()),
                input_buffer_.size()};
        const auto [status, bytes_consumed, args, error_message]{
                parser_.try_parse(request)};

        if (bytes_consumed > 0) {
          std::println("Bytes consumed: {}", bytes_consumed);
          input_buffer_.erase(input_buffer_.begin(),
                              input_buffer_.begin() + bytes_consumed);
        }

        if (status == RespDecoder::Status::Incomplete) {
          std::println("Incomplete");
          break;
        }
        if (status == RespDecoder::Status::Error) {
          parser_.reset();
          queue_response(
                  RespEncoder::encode_simple_error("ERR " + error_message));
          input_buffer_.clear();
          break;
        }
        if (status == RespDecoder::Status::Complete) {
          parser_.reset();
          std::println("Complete");
          for (auto const &arg: args) {
            std::println("Received: {}", arg);
          }
          RedisCommand cmd(args);
          queue_response(p_redis_executor_->execute(cmd));
        }
      }
      continue;
    }
    if (bytes_read == 0) {
      handle_close();
      return;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return;
    }
    handle_error({errno, std::system_category()});
    return;
  }
}

void TcpConnection::handle_write()
{
  while (!output_buffer_.empty()) {
    auto const bytes_written{
            send(fd_, output_buffer_.data(), output_buffer_.size(), 0)};
    if (bytes_written > 0) {
      output_buffer_.erase(0, static_cast<std::size_t>(bytes_written));
      continue;
    }
    if (bytes_written == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    handle_error({errno, std::system_category()});
    return;
  }

  event_loop_.modify(fd_, EPOLLIN | EPOLLRDHUP);
}

void TcpConnection::handle_close()
{
  int const closed_fd{fd_};
  close_connection();
  if (on_close_) {
    on_close_(closed_fd);
  }
}

void TcpConnection::handle_error(std::error_code const error)
{
  std::println(stderr, "connection error on fd {}: {}", fd_, error.message());
  handle_close();
}

void TcpConnection::queue_response(std::string const &response)
{
  output_buffer_ += response;
  event_loop_.modify(fd_, EPOLLIN | EPOLLOUT | EPOLLRDHUP);
}

void TcpConnection::close_connection() noexcept
{
  if (fd_ == -1) {
    return;
  }

  int const fd_to_close{fd_};
  fd_ = -1;
  try {
    event_loop_.remove(fd_to_close);
  } catch (...) {
  }
  close(fd_to_close);
}
