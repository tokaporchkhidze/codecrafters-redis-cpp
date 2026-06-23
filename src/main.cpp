#include <charconv>
#include <expected>
#include <memory>
#include <optional>
#include <print>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include "redis_core/redis-executor.h"
#include "redis_core/services/replication-manager.h"
#include "redis_net/event-loop.h"
#include "redis_net/master-link.h"
#include "redis_net/tcp-server.h"
#include "redis_storage/redis-store.h"

using namespace redis_storage;

namespace
{

constexpr auto k_default_port{6379};

struct ParsedArguments
{
  std::optional<int> port;
  std::optional<std::string> master_host;
  std::optional<int> master_port;
};

std::expected<int, std::string> parse_int(std::string_view value)
{
  int result{};
  auto const *begin{value.data()};
  auto const *end{value.data() + value.size()};
  auto const [ptr, error]{std::from_chars(begin, end, result)};
  if (error != std::errc{} || ptr != end) {
    return std::unexpected(std::format("invalid integer '{}'", value));
  }
  return result;
}

std::string to_string(std::ranges::input_range auto &&range)
{
  auto chars{range | std::views::common};
  return {chars.begin(), chars.end()};
}

std::expected<ParsedArguments, std::string>
parse_arguments(std::vector<std::string> const &args)
{
  ParsedArguments parsed_args{};
  for (int i{1}; i < args.size(); ++i) {
    if (auto const &arg{args[i]}; arg == "--port") {
      if (i + 1 < args.size()) {
        auto port{parse_int(args[++i])};
        if (!port) {
          return std::unexpected(std::format("Err: '--port' invalid value - {}",
                                             port.error()));
        }
        parsed_args.port = *port;
      }
    } else if (arg == "--replicaof") {
      if (i + 1 < args.size()) {
        auto parts{std::vector<std::string>{}};
        auto tokens{args[++i] | std::views::split(' ') |
                    std::views::filter([](auto const &part)
                                       { return !std::ranges::empty(part); }) |
                    std::views::transform([](auto &&part)
                                          { return to_string(part); })};
        std::ranges::copy(tokens, std::back_inserter(parts));
        if (parts.size() != 2U) {
          return std::unexpected(std::format("Err: '--replicaof' invalid "
                                             "format - expected 'host port'"));
        }
        auto master_port{parse_int(parts[1])};
        if (!master_port) {
          return std::unexpected(
                  std::format("Err: '--replicaof' invalid port - {}",
                              master_port.error()));
        }
        parsed_args.master_host = std::move(parts[0]);
        parsed_args.master_port = *master_port;
      }
    }
  }
  return parsed_args;
}

} // namespace

int main(int argc, char *argv[])
{
  try {
    auto const parsed_args{
            parse_arguments(std::vector<std::string>(argv, argv + argc))};
    if (!parsed_args) {
      std::println(stderr, "{}", parsed_args.error());
      return 1;
    }
    redis_net::EventLoop event_loop;
    auto p_redis_store = std::make_shared<RedisStore>();
    auto p_replication =
            std::make_shared<redis_core::redis_command::ReplicationManager>(
                    parsed_args->master_host, parsed_args->master_port);
    auto p_redis_executor =
            std::make_shared<RedisExecutor>(p_redis_store, p_replication);
    event_loop.set_timer(
            [p_redis_executor]
            { return p_redis_executor->get_next_blocked_client_timeout(); },
            [p_redis_executor](
                    redis_net::EventLoop::EventLoopClock::time_point const tp)
            { p_redis_executor->expire_blocked_clients(tp); });
    auto const listening_port{parsed_args->port.value_or(k_default_port)};
    redis_net::TcpServer server{event_loop, p_redis_executor};
    if (auto started{server.start(listening_port, 5)}; !started) {
      std::println(stderr, "failed to start server: {}", started.error());
      return 1;
    }

    std::unique_ptr<redis_net::MasterLink> p_master_link;
    if (!p_replication->is_master()) {
      p_master_link = std::make_unique<redis_net::MasterLink>(
              event_loop,
              *parsed_args->master_host,
              *parsed_args->master_port,
              listening_port);
      p_master_link->start();
    }

    std::println("Waiting for clients to connect...");
    event_loop.run();
  } catch (std::exception const &error) {
    std::println(stderr, "fatal error: {}", error.what());
    return 1;
  }

  return 0;
}
