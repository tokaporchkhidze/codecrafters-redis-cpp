#ifndef REDIS_CPP_REDIS_EXECUTOR_H
#define REDIS_CPP_REDIS_EXECUTOR_H

#include <expected>
#include <functional>
#include <memory>
#include <queue>
#include <string>

#include <unordered_set>
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

  ExecutionResult execute(RedisCommand &cmd, CommandContext const& ctx);

  void on_close_clean_up(int fd);

  void expire_blocked_clients(std::chrono::steady_clock::time_point now);

  std::optional<std::chrono::steady_clock::time_point>
  get_next_blocked_client_timeout();

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

  struct RedisReply;

  struct Array
  {
    std::vector<RedisReply> values;
  };

  struct NullArray
  {
  };

  struct RedisReply
  {
    using Value = std::variant<std::monostate,
                               SimpleString,
                               BulkString,
                               SimpleError,
                               Integer,
                               NullBulkString,
                               Array,
                               NullArray>;

    Value value;

    // Not adding explicit keyword intentionally.
    template<class T>
    RedisReply(T reply) : value(std::move(reply))
    {
    }
  };

  struct ExecutionOutcome
  {
    ResultType type{};
    RedisReply reply;
  };

  struct XReadOptions
  {
    bool blocking{};
    std::optional<std::chrono::steady_clock::time_point> timeout_tp;
    std::span<std::string const> stream_args;
  };

  struct XReadRequest
  {
    bool blocking{};
    std::optional<std::chrono::steady_clock::time_point> timeout_tp;
    std::vector<std::string> stream_keys;
    std::vector<std::string> start_ids;
  };

  using Handler = ExecutionOutcome (RedisExecutor::*)(
          std::span<std::string const> args, CommandContext const& ctx);

  enum class TransactionPolicy
  {
    QUEUE,
    EXECUTE_IMMEDIATELY,
  };

  struct CommandSpec
  {
    // Defined explicitly, so we can in-place construct
    // this struct in the handlers_
    CommandSpec(int const min_argc,
                std::optional<int> const max_argc,
                Handler const handler,
                TransactionPolicy const transaction_policy =
                        TransactionPolicy::QUEUE) :
        min_argc{min_argc},
        max_argc{max_argc},
        handler{handler},
        transaction_policy{transaction_policy}
    {
    }

    int min_argc{};
    std::optional<int> max_argc{};
    Handler handler{};
    TransactionPolicy transaction_policy{};
  };

  struct BlockedClient;

  enum class UnblockOpStatus
  {
    READY,
    NOT_READY_CONTINUE,
    NOT_READY_STOP,
  };

  struct UnblockOpResult
  {
    UnblockOpStatus status{};
    RedisReply reply{std::monostate{}};
  };

  using UnblockOp = std::function<UnblockOpResult(std::string const &,
                                                  BlockedClient const &)>;

  struct BlockedClient
  {
    BlockedClient(
            int const fd,
            std::function<void(std::string)> reply_callback,
            std::optional<std::chrono::steady_clock::time_point> const deadline,
            UnblockOp op) :
        client_fd(fd),
        callback(std::move(reply_callback)),
        timeout_tp(deadline),
        unblock_op(std::move(op))
    {
    }

    int client_fd;
    std::vector<std::string> keys;
    ReplyCallback callback;
    std::optional<std::chrono::steady_clock::time_point> timeout_tp;
    UnblockOp unblock_op;
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

  struct BlockedClientTimeout
  {
    std::chrono::steady_clock::time_point deadline;
    std::weak_ptr<BlockedClient> p_blocked_client;
  };

  struct TimeoutGreater
  {
    bool operator()(BlockedClientTimeout const &a,
                    BlockedClientTimeout const &b) const
    {
      return a.deadline > b.deadline;
    }
  };

  struct TransactionCommand
  {
    TransactionCommand(Handler const handler,
                       std::vector<std::string> args,
                       CommandContext ctx) :
        handler{handler}, args{std::move(args)}, ctx(std::move(ctx))
    {
    }
    Handler handler;
    std::vector<std::string> args;
    CommandContext ctx;
  };

  enum class WatchKeyState
  {
    NOT_MODIFIED,
    MODIFIED
  };

  redis_storage::RedisStorePtr p_store_;
  CommandHandlers handlers_;
  std::unordered_map<std::string, std::deque<std::weak_ptr<BlockedClient>>>
          blocked_clients_by_key_;
  std::unordered_map<int, std::shared_ptr<BlockedClient>>
          blocked_clients_by_fd_;
  std::priority_queue<BlockedClientTimeout,
                      std::vector<BlockedClientTimeout>,
                      TimeoutGreater>
          blocked_clients_timeout_;

  std::unordered_map<int, std::queue<TransactionCommand>>
          clients_transaction_queue_;

  std::unordered_map<std::string, std::unordered_set<int>>
          watched_clients_by_key_;

  std::unordered_map<int, std::unordered_set<std::string>>
          watched_keys_by_clients_;

  std::unordered_set<int> dirty_clients_;

  ExecutionOutcome execute_ping(std::span<std::string const> args,
                                CommandContext const &ctx);
  ExecutionOutcome execute_set(std::span<std::string const> args,
                               CommandContext const& ctx);
  ExecutionOutcome execute_get(std::span<std::string const> args,
                               CommandContext const& ctx);
  ExecutionOutcome execute_incr(std::span<std::string const> args,
                                CommandContext const& ctx);
  ExecutionOutcome execute_echo(std::span<std::string const> args,
                                CommandContext const& ctx);
  ExecutionOutcome execute_rpush(std::span<std::string const> args,
                                 CommandContext const& ctx);
  ExecutionOutcome execute_lpush(std::span<std::string const> args,
                                 CommandContext const& ctx);
  ExecutionOutcome execute_llen(std::span<std::string const> args,
                                CommandContext const& ctx);
  ExecutionOutcome execute_lrange(std::span<std::string const> args,
                                  CommandContext const& ctx);
  ExecutionOutcome execute_lpop(std::span<std::string const> args,
                                CommandContext const& ctx);
  ExecutionOutcome execute_blpop(std::span<std::string const> args,
                                 CommandContext const& ctx);
  ExecutionOutcome execute_type(std::span<std::string const> args,
                                CommandContext const& ctx);
  ExecutionOutcome execute_xadd(std::span<std::string const> args,
                                CommandContext const& ctx);
  ExecutionOutcome execute_xrange(std::span<std::string const> args,
                                  CommandContext const& ctx);
  ExecutionOutcome execute_xread(std::span<std::string const> args,
                                 CommandContext const& ctx);
  ExecutionOutcome execute_multi(std::span<std::string const> args,
                                 CommandContext const& ctx);
  ExecutionOutcome execute_exec(std::span<std::string const> args,
                                CommandContext const& ctx);
  ExecutionOutcome execute_discard(std::span<std::string const> args,
                                   CommandContext const& ctx);
  ExecutionOutcome execute_watch(std::span<std::string const> args,
                                 CommandContext const& ctx);
  ExecutionOutcome execute_unwatch(std::span<std::string const> args,
                                   CommandContext const& ctx);

  std::expected<XReadOptions, std::string>
  parse_xread_options(std::span<std::string const> args) const;
  std::expected<XReadRequest, std::string>
  make_xread_request(XReadOptions const &options);
  std::expected<std::vector<RedisReply>, std::string>
  read_xread_streams(std::vector<std::string> const &stream_keys,
                     std::vector<std::string> const &start_ids) const;
  void block_xread_client(CommandContext const& ctx, XReadRequest request);

  RedisReply
  make_stream_entry_reply(redis_storage::StreamEntry const &entry) const;
  RedisReply make_stream_entries_reply(
          std::vector<redis_storage::StreamEntry> const &entries) const;
  RedisReply make_xread_stream_reply(
          std::string const &key,
          std::vector<redis_storage::StreamEntry> const &entries) const;

  std::string encode_reply(RedisReply const &reply);

  void unblock_client_for_key(std::string const &key);

  void clear_watched_keys(int client_fd);

  void mark_watched_key_dirty(std::string const &key);
};

using RedisExecutorPtr = std::shared_ptr<RedisExecutor>;

} // namespace redis_core

#endif // REDIS_CPP_REDIS_EXECUTOR_H
