#include <cerrno>
#include <cstring>
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

namespace
{

size_t constexpr k_initial_input_buffer_size{1024};
size_t constexpr k_max_idle_input_buffer_size{65536};
size_t constexpr k_min_size_to_compact{8192};
size_t constexpr k_input_buffer_overflow_limit{16 * 1024 * 1024};

} // namespace

TcpConnection::TcpConnection(EventLoop &event_loop,
                             int const fd,
                             CloseCallback on_close,
                             RedisExecutorPtr p_redis_executor) :
    event_loop_{event_loop},
    fd_{fd},
    p_redis_executor_(std::move(p_redis_executor)),
    on_close_{std::move(on_close)}
{
  input_buffer_.resize(k_initial_input_buffer_size);
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
  while (true) {
    if (!allocate_input_buffer_if()) {
      close_after_write_ = true;
      queue_response(RespEncoder::encode_simple_error("ERR too much data"));
      return;
    }
    auto const bytes_read{recv(fd_,
                               input_buffer_.data() + input_write_pos_,
                               input_buffer_.size() - input_write_pos_,
                               0)};
    if (bytes_read > 0) {
      input_write_pos_ += bytes_read;

      auto current_buffer{std::string_view{
              reinterpret_cast<char const *>(input_buffer_.data()) +
                      input_read_pos_,
              input_write_pos_ - input_read_pos_}};

      while (!current_buffer.empty()) {
        auto const [status, bytes_consumed, args, error_message]{
                parser_.try_parse(current_buffer)};

        if (bytes_consumed > 0) {
          current_buffer = current_buffer.substr(bytes_consumed);
          input_read_pos_ += bytes_consumed;
        }

        if (status == RespDecoder::Status::Incomplete) {
          break;
        }
        if (status == RespDecoder::Status::Error) {
          close_after_write_ = true;
          queue_response(
                  RespEncoder::encode_simple_error("ERR " + error_message));
          return;
        }
        if (status == RespDecoder::Status::Complete) {
          parser_.reset();
          RedisCommand cmd(args);
          auto self_weak{weak_from_this()};
          const auto [type, reply]{p_redis_executor_->execute(
                  cmd,
                  RedisExecutor::CommandContext{
                          fd_,
                          [self_weak](std::string const &response)
                          {
                            if (auto const self{self_weak.lock()}; self)
                              self->queue_response(response);
                          }})};
          if (type == RedisExecutor::ResultType::REPLY) {
            queue_response(reply);
          } else {
            event_loop_.modify(fd_, EPOLLRDHUP);
            // if client pipelined multiple commands,
            // let's stop after first blocking command,
            // only continue after unblocking happened.
            // TODO: If we receive multiple pipelined commands
            //  and blocking happens, after unblocking we rely on
            // EPOLLIN for resuming the read flow, which may not happen.
            // need some kind of resume path which will drain the buffer
            // if it already contains full commands.
            return;
          }
        }
      }
      compact_input_buffer_if();
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
    auto const &response{output_buffer_.front()};
    auto const bytes_written{send(fd_,
                                  response.data() + output_front_offset_,
                                  response.size() - output_front_offset_,
                                  0)};
    if (bytes_written > 0) {
      output_front_offset_ += static_cast<size_t>(bytes_written);
      if (output_front_offset_ == response.size()) {
        output_buffer_.pop();
        output_front_offset_ = 0;
      }
      continue;
    }
    if (bytes_written == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    handle_error({errno, std::system_category()});
    return;
  }

  if (output_buffer_.empty() && close_after_write_) {
    handle_close();
    return;
  }

  event_loop_.modify(fd_, EPOLLIN | EPOLLRDHUP);
}

void TcpConnection::handle_close()
{
  int const closed_fd{fd_};
  close_connection();
  p_redis_executor_->remove_blocked_client(closed_fd);
  if (on_close_) {
    on_close_(closed_fd);
  }
}

void TcpConnection::handle_error(std::error_code const error)
{
  std::println(stderr, "connection error on fd {}: {}", fd_, error.message());
  handle_close();
}

void TcpConnection::queue_response(std::string response)
{
  output_buffer_.emplace(std::move(response));
  auto events{EPOLLOUT | EPOLLRDHUP};
  if (!close_after_write_) {
    events |= EPOLLIN;
  }
  event_loop_.modify(fd_, events);
}

bool TcpConnection::allocate_input_buffer_if()
{
  if (input_write_pos_ < input_buffer_.size()) {
    return true;
  }
  compact_input_buffer_if();

  if (input_write_pos_ == input_buffer_.size()) {
    auto const new_size{input_buffer_.size() * 2};
    if (new_size > k_input_buffer_overflow_limit) {
      return false;
    }
    input_buffer_.resize(new_size);
  }
  return true;
}

void TcpConnection::compact_input_buffer_if()
{
  if (input_read_pos_ == 0) {
    return;
  }

  if (input_read_pos_ == input_write_pos_) {
    // Full read, reset indexes.
    input_read_pos_ = 0;
    input_write_pos_ = 0;
    if (input_buffer_.size() >= k_max_idle_input_buffer_size) {
      input_buffer_.resize(k_initial_input_buffer_size);
      input_buffer_.shrink_to_fit();
    }
  }

  if (input_read_pos_ >= k_min_size_to_compact &&
      input_read_pos_ >= input_buffer_.size() / 2) {
    auto const in_progress_bytes{input_write_pos_ - input_read_pos_};
    std::memmove(input_buffer_.data(),
                 input_buffer_.data() + input_read_pos_,
                 in_progress_bytes);
    input_read_pos_ = 0;
    input_write_pos_ = in_progress_bytes;
  }
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
