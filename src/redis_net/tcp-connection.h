#ifndef REDIS_CPP_TCP_CONNECTION_H
#define REDIS_CPP_TCP_CONNECTION_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "redis_core/resp-decoder.h"
#include "event-loop.h"

namespace redis_net
{

class TcpConnection : public std::enable_shared_from_this<TcpConnection>
{
public:
  using CloseCallback = std::function<void(int)>;

  TcpConnection(EventLoop &event_loop, int fd, CloseCallback on_close);
  ~TcpConnection() noexcept;

  TcpConnection(TcpConnection const &) = delete;
  TcpConnection &operator=(TcpConnection const &) = delete;

  [[nodiscard]] int fd() const;
  void start();

private:
  EventLoop &event_loop_;
  int fd_{-1};
  std::vector<uint8_t> input_buffer_{};
  redis_core::RespDecoder parser_{};
  
  std::string output_buffer_{};
  CloseCallback on_close_;

  void handle_read();
  void handle_write();
  void handle_close();
  void handle_error(std::error_code error);
  void queue_response(std::string const& response);
  void close_connection() noexcept;
};

} // namespace redis_net

#endif // REDIS_CPP_TCP_CONNECTION_H
