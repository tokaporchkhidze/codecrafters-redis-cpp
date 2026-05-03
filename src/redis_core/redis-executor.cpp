#include "redis-executor.h"

#include <format>
#include "resp-encoder.h"

using namespace redis_core;

RedisExecutor::RedisExecutor(RedisStorePtr p_redis_store) :
    p_redis_store_(std::move(p_redis_store))
{
}

std::string RedisExecutor::execute(RedisCommand const &cmd)
{
  // TODO: Just simple implementation of execute
  auto const &args{cmd.args()};
  if (auto const name{cmd.name()}; name == "SET") {
    if (args.size() < 2) {
      return RespEncoder::encode_simple_error("ERR wrong number of arguments");
    }
    RedisStore::SetOptions options;
    if (args.size() == 4 && args[2] == "PX") {
      options.ttl_ms = std::chrono::milliseconds{std::stoi(args[3])};
    }
    p_redis_store_->set(args[0], args[1], options);
    return RespEncoder::encode_simple_string("OK");
  } else if (name == "GET") {
    if (args.size() != 1) {
      return RespEncoder::encode_simple_error("ERR wrong number of arguments");
    }
    if (auto const value{p_redis_store_->get(args[0])}; value.has_value()) {
      return RespEncoder::encode_bulk_string(value.value());
    }
    return RespEncoder::encode_null_string();
  } else if (name == "PING") {
    return RespEncoder::encode_simple_string("PONG");
  } else if (name == "ECHO") {
    if (args.empty()) {
      return RespEncoder::encode_simple_string("");
    }
    return RespEncoder::encode_bulk_string(args[0]);
  } else {
    return RespEncoder::encode_simple_error(
            std::format("ERR unknown command {}", name));
  }
}
