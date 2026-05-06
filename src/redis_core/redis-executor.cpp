#include "redis-executor.h"

#include <algorithm>
#include <format>
#include <functional>
#include <ranges>

#include "resp-encoder.h"

using namespace redis_core;
using namespace redis_storage;

namespace
{

template<class... Ts>
struct Overloaded : Ts...
{
  using Ts::operator()...;
};

} // namespace

RedisExecutor::RedisExecutor(RedisStorePtr p_redis_store) :
    p_store_(std::move(p_redis_store))
{
  handlers_.try_emplace("PING", 0, 0, &RedisExecutor::execute_ping);
  handlers_.try_emplace("ECHO", 0, 1, &RedisExecutor::execute_echo);
  handlers_.try_emplace("SET", 2, 4, &RedisExecutor::execute_set);
  handlers_.try_emplace("GET", 1, 1, &RedisExecutor::execute_get);
  handlers_.try_emplace(
          "RPUSH", 2, std::nullopt, &RedisExecutor::execute_rpush);
  handlers_.try_emplace(
          "LPUSH", 2, std::nullopt, &RedisExecutor::execute_lpush);
  handlers_.try_emplace("LLEN", 1, 1, &RedisExecutor::execute_llen);
  handlers_.try_emplace("LRANGE", 3, 3, &RedisExecutor::execute_lrange);
  handlers_.try_emplace("LPOP", 1, 2, &RedisExecutor::execute_lpop);
  handlers_.try_emplace(
          "BLPOP", 1, std::nullopt, &RedisExecutor::execute_blpop);
}

RedisExecutor::ExecutionResult RedisExecutor::execute(RedisCommand const &cmd,
                                                      CommandContext ctx)
{
  auto const &args{cmd.args()};
  if (auto const it{handlers_.find(cmd.name())}; it != handlers_.cend()) {
    auto &[min_argc, max_argc_op, handler] = it->second;
    if (args.size() < min_argc || args.size() > max_argc_op.value_or(INT_MAX)) {
      return ExecutionResult{
              ResultType::REPLY,
              encode_reply(SimpleError(std::format(
                      "Invalid number of arguments - {}", args.size())))};
    }
    auto [type, reply] = std::invoke(handler, this, args, ctx);
    return ExecutionResult{type, encode_reply(reply)};
  }
  return ExecutionResult{ResultType::REPLY,
                         encode_reply(SimpleError("Unknown command"))};
}

void RedisExecutor::remove_blocked_client(int const fd)
{
  if (blocked_clients_by_fd_.contains(fd))
  {
    blocked_clients_by_fd_.erase(fd);
  }
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_ping(std::span<std::string const> const, CommandContext)
{
  return ExecutionOutcome{ResultType::REPLY, SimpleString("PONG")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_set(std::span<std::string const> const args,
                           CommandContext)
{
  // TODO: Need to normalize accepted argument to uppercase,
  // so if users gives "px", we do not silently skip it.
  std::string_view constexpr px_arg{"PX"};
  RedisStore::SetOptions options;
  // TODO: Currently only supporting passive expiration.
  // I need to implement active one as well..................
  if (args.size() == 4 && args[2] == px_arg) {
    options.ttl_ms = std::chrono::milliseconds{std::stoi(args[3])};
  }
  p_store_->set(args[0], args[1], options);
  return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_get(std::span<std::string const> const args,
                           CommandContext)
{
  if (auto const value{p_store_->get(args[0])}; value.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, BulkString(value.value())};
  }
  return ExecutionOutcome{ResultType::REPLY, NullBulkString{}};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_echo(std::span<std::string const> const args,
                            CommandContext)
{
  if (args.empty()) {
    return ExecutionOutcome{ResultType::REPLY, BulkString("")};
  }
  return ExecutionOutcome{ResultType::REPLY, BulkString(args[0])};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_rpush(std::span<std::string const> const args,
                             CommandContext)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const new_size{p_store_->rpush(args[0], args.subspan(1))};
      new_size.has_value()) {
    unblock_client_for_key(args[0]);
    return ExecutionOutcome{ResultType::REPLY, Integer{new_size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to push to the list")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_lpush(std::span<std::string const> const args,
                             CommandContext)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const new_size{p_store_->lpush(args[0], args.subspan(1))};
      new_size.has_value()) {
    unblock_client_for_key(args[0]);
    return ExecutionOutcome{ResultType::REPLY, Integer{new_size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to push to the list")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_llen(std::span<std::string const> const args,
                            CommandContext)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const size{p_store_->llen(args[0])}; size.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, Integer{size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to get the list length")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_lrange(std::span<std::string const> const args,
                              CommandContext)
{
  return ExecutionOutcome{
          ResultType::REPLY,
          Array(std::move(p_store_->lrange(
                  args[0], std::stoll(args[1]), std::stoll(args[2]))))};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_lpop(std::span<std::string const> const args,
                            CommandContext)
{
  bool const has_count_arg = args.size() == 2;
  int64_t const count = has_count_arg ? std::stoll(args[1]) : 1;
  if (count < 0) {
    return ExecutionOutcome{
            ResultType::REPLY,
            SimpleError("value is out of range, must be positive")};
  }
  auto popped = p_store_->lpop(args[0], count);
  if (!popped.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, SimpleError("Wrong type")};
  }

  auto elements = std::move(popped.value());
  if (elements.empty()) {
    return ExecutionOutcome{ResultType::REPLY, NullBulkString{}};
  }

  if (has_count_arg) {
    return ExecutionOutcome{ResultType::REPLY, Array(std::move(elements))};
  }

  return ExecutionOutcome{ResultType::REPLY,
                          BulkString(std::move(elements.front()))};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_blpop(std::span<std::string const> args,
                             CommandContext ctx)
{
  for (auto const &key: args) {
    auto popped = p_store_->lpop(key, 1);
    if (!popped.has_value()) {
      continue;
    }
    auto elements = std::move(popped.value());
    if (elements.empty()) {
      continue;
    }
    return ExecutionOutcome{ResultType::REPLY, Array({key, elements.front()})};
  }
  // If we get here, means no pop happened, need to block.
  auto blocked_client{std::make_shared<BlockedClient>(
          ctx.client_fd,
          std::vector<std::string>{args.begin(), args.end()},
          ctx.callback)};
  blocked_clients_by_fd_.emplace(ctx.client_fd, blocked_client);
  for (auto const &key: args) {
    auto [it, inserted]{blocked_clients_by_key_.try_emplace(key)};
    it->second.emplace_back(blocked_client);
  }
  return ExecutionOutcome{ResultType::BLOCKED, NullBulkString{}};
}

std::string RedisExecutor::encode_reply(RedisReply const &reply)
{
  return std::visit(
          Overloaded{
                  [](SimpleString const &val)
                  { return RespEncoder::encode_simple_string(val.value); },
                  [](BulkString const &val)
                  { return RespEncoder::encode_bulk_string(val.value); },
                  [](NullBulkString const &)
                  { return RespEncoder::encode_null_string(); },
                  [](SimpleError const &val)
                  { return RespEncoder::encode_simple_error(val.value); },
                  [](Integer const &val)
                  { return RespEncoder::encode_integer(val.value); },
                  [](Array const &val)
                  { return RespEncoder::encode_array(val.values); },
          },
          reply);
}

void RedisExecutor::unblock_client_for_key(std::string const &key)
{
  if (auto const it{blocked_clients_by_key_.find(key)};
      it != blocked_clients_by_key_.cend()) {
    auto &blocked_clients = it->second;
    while (!blocked_clients.empty()) {
      auto const p_blocked_client{blocked_clients.front().lock()};
      if (!p_blocked_client) {
        blocked_clients.pop_front();
        continue;
      }

      auto popped = p_store_->lpop(key, 1);
      if (!popped.has_value() || popped.value().empty()) {
        break;
      }

      blocked_clients.pop_front();
      int const client_fd{p_blocked_client->client_fd};
      p_blocked_client->callback(
              encode_reply(Array({key, popped.value().front()})));
      blocked_clients_by_fd_.erase(client_fd);
    }

    if (blocked_clients.empty()) {
      blocked_clients_by_key_.erase(it);
    }
  }
}
