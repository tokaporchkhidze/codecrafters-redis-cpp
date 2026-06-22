#ifndef REDIS_CPP_BLOCKING_MANAGER_H
#define REDIS_CPP_BLOCKING_MANAGER_H

#include <chrono>
#include <deque>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <queue>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "../command-context.h"
#include "../commands/command-interface.h"
#include "blocking-service.h"
#include "redis_storage/redis-store.h"

namespace redis_core::redis_command
{

// Owns all blocking-command state (BLPOP / blocking XREAD) and the
// per-key blocked-client bookkeeping that was previously embedded in
// RedisExecutor. Implements IBlockingService so commands can reach it
// through CommandDeps.
class BlockingManager : public IBlockingService
{
public:
  using TransactionQuery = std::function<bool(int)>;

  BlockingManager(redis_storage::RedisStorePtr store,
                  TransactionQuery is_in_transaction);

  void unblock_client_for_key(std::string const &key) override;
  ExecutionOutcome run_blpop(std::span<std::string const> args,
                             CommandContext const &ctx) override;
  ExecutionOutcome run_xread(std::span<std::string const> args,
                             CommandContext const &ctx) override;

  void expire_blocked_clients(std::chrono::steady_clock::time_point now);
  std::optional<std::chrono::steady_clock::time_point>
  get_next_blocked_client_timeout();
  void on_close_clean_up(int fd);

private:
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

  ExecutionOutcome execute_lpop(std::span<std::string const> args,
                                CommandContext const &ctx);
  ExecutionOutcome execute_blpop(std::span<std::string const> args,
                                 CommandContext const &ctx);
  ExecutionOutcome execute_xread(std::span<std::string const> args,
                                 CommandContext const &ctx);

  std::expected<XReadOptions, std::string>
  parse_xread_options(std::span<std::string const> args) const;
  std::expected<XReadRequest, std::string>
  make_xread_request(XReadOptions const &options);
  std::expected<std::vector<RedisReply>, std::string>
  read_xread_streams(std::vector<std::string> const &stream_keys,
                     std::vector<std::string> const &start_ids) const;
  void block_xread_client(CommandContext const &ctx, XReadRequest request);

  redis_storage::RedisStorePtr p_store_;
  TransactionQuery is_in_transaction_;

  std::unordered_map<std::string, std::deque<std::weak_ptr<BlockedClient>>>
          blocked_clients_by_key_;
  std::unordered_map<int, std::shared_ptr<BlockedClient>>
          blocked_clients_by_fd_;
  std::priority_queue<BlockedClientTimeout,
                      std::vector<BlockedClientTimeout>,
                      TimeoutGreater>
          blocked_clients_timeout_;
};

} // namespace redis_core::redis_command

#endif // REDIS_CPP_BLOCKING_MANAGER_H
