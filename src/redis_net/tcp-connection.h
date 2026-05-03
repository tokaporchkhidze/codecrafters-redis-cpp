#ifndef REDIS_CPP_TCP_CONNECTION_H
#define REDIS_CPP_TCP_CONNECTION_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "event-loop.h"
#include "redis_core/redis-executor.h"
#include "redis_core/resp-decoder.h"

using namespace redis_core;

namespace redis_net
{

class TcpConnection : public std::enable_shared_from_this<TcpConnection>
{
public:
  using CloseCallback = std::function<void(int)>;

  TcpConnection(EventLoop &event_loop,
                int fd,
                CloseCallback on_close,
                RedisExecutorPtr p_redis_executor);
  ~TcpConnection() noexcept;

  TcpConnection(TcpConnection const &) = delete;
  TcpConnection &operator=(TcpConnection const &) = delete;

  [[nodiscard]] int fd() const;
  void start();

private:
  EventLoop &event_loop_;
  int fd_{-1};
  std::vector<uint8_t> input_buffer_{};
  RespDecoder parser_{};
  RedisExecutorPtr p_redis_executor_;

  std::string output_buffer_{};
  CloseCallback on_close_;

  void handle_read();
  void handle_write();
  void handle_close();
  void handle_error(std::error_code error);
  void queue_response(std::string const &response);
  void close_connection() noexcept;
};

} // namespace redis_net

#endif // REDIS_CPP_TCP_CONNECTION_H
