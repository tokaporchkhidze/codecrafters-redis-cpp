#include "tcp-connection.h"

#include <array>
#include <cerrno>
#include <print>
#include <system_error>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

using namespace redis_net;

TcpConnection::TcpConnection(EventLoop &event_loop,
                             int const fd,
                             CloseCallback on_close)
    : event_loop_{event_loop}, fd_{fd}, on_close_{std::move(on_close)}
{
}

TcpConnection::~TcpConnection() noexcept
{
  close_connection();
}

int TcpConnection::fd() const
{
  return fd_;
}

void TcpConnection::start()
{
  auto self{shared_from_this()};
  event_loop_.add(
          fd_,
          EPOLLIN | EPOLLRDHUP,
          EventLoop::Handler{
                  .on_read = [self] { self->handle_read(); },
                  .on_write = [self] { self->handle_write(); },
                  .on_close = [self] { self->handle_close(); },
                  .on_error = [self](std::error_code const error) {
                    self->handle_error(error);
                  },
          });
}

void TcpConnection::handle_read()
{
  std::array<char, 4096> buffer{};
  while (true) {
    auto const bytes_read{recv(fd_, buffer.data(), buffer.size(), 0)};
    if (bytes_read > 0) {
      queue_response("+PONG\r\n");
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

void TcpConnection::queue_response(std::string const& response)
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
