#ifndef REDIS_CPP_REDIS_EXECUTOR_H
#define REDIS_CPP_REDIS_EXECUTOR_H

#include <string>

#include <variant>
#include "redis-command.h"
#include "redis_storage/redis-store.h"

namespace redis_core
{

class RedisExecutor
{
public:
  explicit RedisExecutor(redis_storage::RedisStorePtr p_redis_store);

  std::string execute(RedisCommand const &cmd);

private:
  struct SimpleString
  {
    std::string value;
  };

  struct BulkString
  {
    std::string value;
  };

  struct SimpleError
  {
    std::string value;
  };

  struct NullBulkString
  {
  };

  struct Integer
  {
    int64_t value;
  };

  struct Array
  {
    std::vector<std::string> values;
  };

  using RedisReply = std::variant<SimpleString,
                                  BulkString,
                                  SimpleError,
                                  Integer,
                                  NullBulkString,
                                  Array>;
  using Handler =
          RedisReply (RedisExecutor::*)(std::span<std::string const> args);

  struct CommandSpec
  {
    // Defined explicitly, so we can in-place construct
    // this struct in the handlers_
    CommandSpec(int const min_argc,
                std::optional<int> const max_argc,
                Handler const handler) :
        min_argc{min_argc}, max_argc{max_argc}, handler{handler}
    {
    }

    int min_argc{};
    std::optional<int> max_argc{};
    Handler handler{};
  };

  // For enabling, looking up commands with string_view_s.
  struct StringHash
  {
    using is_transparent = void;

    std::size_t operator()(std::string_view const value) const
    {
      return std::hash<std::string_view>{}(value);
    }
  };

  using CommandHandlers = std::
          unordered_map<std::string, CommandSpec, StringHash, std::equal_to<>>;


  RedisReply execute_ping(std::span<std::string const> args);
  RedisReply execute_set(std::span<std::string const> args);
  RedisReply execute_get(std::span<std::string const> args);
  RedisReply execute_echo(std::span<std::string const> args);
  RedisReply execute_rpush(std::span<std::string const> args);
  RedisReply execute_lpush(std::span<std::string const> args);
  RedisReply execute_lrange(std::span<std::string const> args);

  std::string encode_reply(RedisReply const &reply);


  redis_storage::RedisStorePtr p_store_;
  CommandHandlers handlers_;
};

using RedisExecutorPtr = std::shared_ptr<RedisExecutor>;

} // namespace redis_core

#endif // REDIS_CPP_REDIS_EXECUTOR_H
