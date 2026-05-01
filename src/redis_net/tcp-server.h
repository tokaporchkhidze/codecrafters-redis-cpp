#ifndef REDIS_STARTER_CPP_TCP_SERVER_H
#define REDIS_STARTER_CPP_TCP_SERVER_H
#include <expected>
#include <memory>
#include <string>
#include <unordered_map>

#include "event-loop.h"
#include "tcp-connection.h"

namespace redis_net
{

class TcpServer
{
public:
  explicit TcpServer(EventLoop &event_loop);
  ~TcpServer() noexcept;

  TcpServer(TcpServer const &) = delete;
  TcpServer &operator=(TcpServer const &) = delete;

  [[nodiscard]] std::expected<void, std::string> start(int port,
                                                       int conn_backlog);

private:
  EventLoop &event_loop_;
  int server_fd_{-1};
  std::unordered_map<int, std::shared_ptr<TcpConnection>> connections_{};

  void handle_accept();
  void remove_connection(int fd);
  void close_server() noexcept;
};

} // namespace redis_net


#endif // REDIS_STARTER_CPP_TCP_SERVER_H
