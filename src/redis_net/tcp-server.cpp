#include "tcp-server.h"

#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <print>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>

#include "errno-error.h"

using namespace redis_net;

namespace
{

void set_nonblocking(int const fd)
{
  int const flags{fcntl(fd, F_GETFL, 0)};
  if (flags == -1) {
    throw std::system_error{errno, std::system_category()};
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    throw std::system_error{errno, std::system_category()};
  }
}

} // namespace

TcpServer::TcpServer(EventLoop &event_loop, RedisExecutorPtr p_redis_executor) :
    event_loop_(event_loop), p_redis_executor_(std::move(p_redis_executor))
{
  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ == -1) {
    throw std::system_error{errno, std::system_category()};
  }
  set_nonblocking(server_fd_);

  int constexpr reuse = 1;
  if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) ==
      -1) {
    int const err{errno};
    throw std::system_error{err, std::system_category()};
  }
}

std::expected<void, std::string> TcpServer::start(int const port,
                                                  int const conn_backlog)
{
  sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(port);
  if (bind(server_fd_,
           reinterpret_cast<sockaddr *>(&server_addr),
           sizeof(server_addr)) == -1) {
    return unexpected_errno();
  }
  if (listen(server_fd_, conn_backlog) == -1) {
    return unexpected_errno();
  }
  event_loop_.add(server_fd_,
                  EPOLLIN,
                  EventLoop::Handler{
                          .on_read = [this] { handle_accept(); },
                          .on_error =
                                  [](std::error_code const error)
                          {
                            std::println(stderr,
                                         "server socket error: {}",
                                         error.message());
                          },
                  });
  return {};
}

void TcpServer::handle_accept()
{
  while (true) {
    auto const client_fd{accept(server_fd_, nullptr, nullptr)};
    if (client_fd == -1) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return;
      }
      print_errno();
      return;
    }

    try {
      set_nonblocking(client_fd);
    } catch (std::system_error const &error) {
      std::println(
              stderr, "failed to configure client socket: {}", error.what());
      close(client_fd);
      continue;
    }

    auto connection{
            std::make_shared<TcpConnection>(
                    event_loop_,
                    client_fd,
                    [this](int const fd) { remove_connection(fd); },
                    p_redis_executor_),
    };
    connections_.emplace(client_fd, connection);
    connection->start();
  }
}

TcpServer::~TcpServer() noexcept { close_server(); }

void TcpServer::remove_connection(int const fd) { connections_.erase(fd); }

void TcpServer::close_server() noexcept
{
  if (server_fd_ != -1) {
    int const fd_to_close{server_fd_};
    server_fd_ = -1;
    try {
      event_loop_.remove(fd_to_close);
    } catch (...) {
    }
    close(fd_to_close);
  }
}
