#include "redis-executor.h"

#include <format>
#include <functional>

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
}

std::string RedisExecutor::execute(RedisCommand const &cmd)
{
  auto const &args{cmd.args()};
  if (auto const it{handlers_.find(cmd.name())}; it != handlers_.cend()) {
    auto &[min_argc, max_argc_op, handler] = it->second;
    if (args.size() < min_argc || args.size() > max_argc_op.value_or(INT_MAX)) {
      return encode_reply(SimpleError(
              std::format("Invalid number of arguments - {}", args.size())));
    }
    return encode_reply(std::invoke(handler, this, args));
  }
  return encode_reply(SimpleError("Unknown command"));
}

RedisExecutor::RedisReply
RedisExecutor::execute_ping(std::span<std::string const> const)
{
  return SimpleString("PONG");
}

RedisExecutor::RedisReply
RedisExecutor::execute_set(std::span<std::string const> const args)
{
  std::string_view constexpr px_arg{"PX"};
  RedisStore::SetOptions options;
  // TODO: Currently only supporting passive expiration.
  // I need to implement active one as well..................
  if (args.size() == 4 && args[2] == px_arg) {
    options.ttl_ms = std::chrono::milliseconds{std::stoi(args[3])};
  }
  p_store_->set(args[0], args[1], options);
  return SimpleString("OK");
}

RedisExecutor::RedisReply
RedisExecutor::execute_get(std::span<std::string const> const args)
{
  if (auto const value{p_store_->get(args[0])}; value.has_value()) {
    return BulkString(value.value());
  }
  return NullBulkString{};
}

RedisExecutor::RedisReply
RedisExecutor::execute_echo(std::span<std::string const> const args)
{
  if (args.empty()) {
    return BulkString("");
  }
  return BulkString(args[0]);
}

RedisExecutor::RedisReply
RedisExecutor::execute_rpush(std::span<std::string const> const args)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const new_size{p_store_->rpush(args[0], args.subspan(1))};
      new_size.has_value()) {
    return Integer{new_size.value()};
  }
  return SimpleError("Failed to push to the list");
}

RedisExecutor::RedisReply
RedisExecutor::execute_lpush(std::span<std::string const> const args)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const new_size{p_store_->lpush(args[0], args.subspan(1))};
      new_size.has_value()) {
    return Integer{new_size.value()};
  }
  return SimpleError("Failed to push to the list");
}

RedisExecutor::RedisReply
RedisExecutor::execute_llen(std::span<std::string const> const args)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const size{p_store_->llen(args[0])}; size.has_value()) {
    return Integer{size.value()};
  }
  return SimpleError("Failed to get the list length");
}

RedisExecutor::RedisReply
RedisExecutor::execute_lrange(std::span<std::string const> const args)
{
  return Array(std::move(
          p_store_->lrange(args[0], std::stoll(args[1]), std::stoll(args[2]))));
}

RedisExecutor::RedisReply
RedisExecutor::execute_lpop(std::span<std::string const> const args)
{
  bool const has_count_arg = args.size() == 2;
  int64_t const count = has_count_arg ? std::stoll(args[1]) : 1;

  auto popped = p_store_->lpop(args[0], count);
  if (!popped.has_value()) {
    return SimpleError("Wrong type");
  }

  auto elements = std::move(popped.value());
  if (elements.empty()) {
    return NullBulkString{};
  }

  if (has_count_arg) {
    return Array(std::move(elements));
  }

  return BulkString(std::move(elements.front()));
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
