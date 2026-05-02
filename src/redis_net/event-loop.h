
#ifndef REDIS_CPP_EVENT_LOOP_H
#define REDIS_CPP_EVENT_LOOP_H
#include <cstdint>
#include <functional>
#include <system_error>
#include <unordered_map>

namespace redis_net
{

class EventLoop
{
public:
  struct Handler
  {
    std::function<void()> on_read;
    std::function<void()> on_write;
    std::function<void()> on_close;
    std::function<void(std::error_code)> on_error;
  };

  EventLoop();
  ~EventLoop() noexcept;

  EventLoop(EventLoop const &) = delete;
  EventLoop &operator=(EventLoop const &) = delete;

  void add(int fd, uint32_t events, Handler handler);
  void modify(int fd, uint32_t events) const;
  void remove(int fd);
  void run();
  void stop();
private:
  std::unordered_map<int, Handler> handlers_{};
  int epoll_fd_{-1};
  bool running_{false};
};

} // namespace redis_net

#endif // REDIS_CPP_EVENT_LOOP_H
