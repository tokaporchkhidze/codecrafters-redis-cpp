#ifndef REDIS_CPP_REDIS_EXECUTOR_H
#define REDIS_CPP_REDIS_EXECUTOR_H

#include <functional>
#include <memory>
#include <queue>
#include <string>

#include <variant>
#include "redis-command.h"
#include "redis_storage/redis-store.h"

namespace redis_core
{

class RedisExecutor
{
public:
  enum class ResultType
  {
    REPLY = 0,
    BLOCKED,
  };

  using ReplyCallback = std::function<void(std::string)>;

  struct CommandContext
  {
    int client_fd;
    ReplyCallback callback;
  };

  struct ExecutionResult
  {
    ResultType type;
    std::string reply;
  };

  explicit RedisExecutor(redis_storage::RedisStorePtr p_redis_store);

  ExecutionResult execute(RedisCommand const &cmd, CommandContext ctx);

  void remove_blocked_client(int fd);

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

  struct ExecutionOutcome
  {
    ResultType type;
    RedisReply reply;
  };

  using Handler = ExecutionOutcome (RedisExecutor::*)(
          std::span<std::string const> args, CommandContext ctx);

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

  struct BlockedClient
  {
    BlockedClient(int const fd,
                  std::vector<std::string> list_keys,
                  std::function<void(std::string)> reply_callback) :
        client_fd(fd),
        keys(std::move(list_keys)),
        callback(std::move(reply_callback))
    {
    }
    int client_fd;
    std::vector<std::string> keys;
    ReplyCallback callback;
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

  ExecutionOutcome execute_ping(std::span<std::string const> args,
                                CommandContext ctx);
  ExecutionOutcome execute_set(std::span<std::string const> args,
                               CommandContext ctx);
  ExecutionOutcome execute_get(std::span<std::string const> args,
                               CommandContext ctx);
  ExecutionOutcome execute_echo(std::span<std::string const> args,
                                CommandContext ctx);
  ExecutionOutcome execute_rpush(std::span<std::string const> args,
                                 CommandContext ctx);
  ExecutionOutcome execute_lpush(std::span<std::string const> args,
                                 CommandContext ctx);
  ExecutionOutcome execute_llen(std::span<std::string const> args,
                                CommandContext ctx);
  ExecutionOutcome execute_lrange(std::span<std::string const> args,
                                  CommandContext ctx);
  ExecutionOutcome execute_lpop(std::span<std::string const> args,
                                CommandContext ctx);
  ExecutionOutcome execute_blpop(std::span<std::string const> args,
                                 CommandContext ctx);

  std::string encode_reply(RedisReply const &reply);

  void unblock_client_for_key(std::string const &key);


  redis_storage::RedisStorePtr p_store_;
  CommandHandlers handlers_;
  std::unordered_map<std::string, std::deque<std::weak_ptr<BlockedClient>>>
          blocked_clients_by_key_;
  std::unordered_map<int, std::shared_ptr<BlockedClient>>
          blocked_clients_by_fd_;
};

using RedisExecutorPtr = std::shared_ptr<RedisExecutor>;

} // namespace redis_core

#endif // REDIS_CPP_REDIS_EXECUTOR_H
