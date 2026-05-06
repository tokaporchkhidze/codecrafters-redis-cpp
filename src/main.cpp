#include <exception>
#include <print>

#include "redis_core/redis-executor.h"
#include "redis_net/event-loop.h"
#include "redis_net/tcp-server.h"
#include "redis_storage/redis-store.h"

using namespace redis_storage;

int main()
{
  try {
    redis_net::EventLoop event_loop;
    auto p_redis_store = std::make_shared<RedisStore>();
    auto p_redis_executor = std::make_shared<RedisExecutor>(p_redis_store);
    event_loop.set_timer(
            [p_redis_executor]
            { return p_redis_executor->get_next_blocked_client_timeout(); },
            [p_redis_executor](
                    redis_net::EventLoop::EventLoopClock::time_point const tp)
            { p_redis_executor->expire_blocked_clients(tp); });
    redis_net::TcpServer server{event_loop, p_redis_executor};

    if (auto started{server.start(6379, 5)}; !started) {
      std::println(stderr, "failed to start server: {}", started.error());
      return 1;
    }

    std::println("Waiting for clients to connect...");
    event_loop.run();
  } catch (std::exception const &error) {
    std::println(stderr, "fatal error: {}", error.what());
    return 1;
  }

  return 0;
}
