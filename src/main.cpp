#include <exception>
#include <print>

#include "redis_net/event-loop.h"
#include "redis_net/tcp-server.h"

int main()
{
  try {
    redis_net::EventLoop event_loop;
    redis_net::TcpServer server{event_loop};

    if (auto started{server.start(6379, 5)}; !started) {
      std::println(stderr, "failed to start server: {}", started.error());
      return 1;
    }

    std::println("Logs from your program will appear here!");
    std::println("Waiting for clients to connect...");
    event_loop.run();
  } catch (std::exception const &error) {
    std::println(stderr, "fatal error: {}", error.what());
    return 1;
  }

  return 0;
}
