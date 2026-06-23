#ifndef REDIS_CPP_TRANSACTION_MANAGER_H
#define REDIS_CPP_TRANSACTION_MANAGER_H

#include <queue>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../command-context.h"
#include "../commands/command-interface.h"
#include "blocking-service.h"
#include "transaction-service.h"
#include "redis_storage/redis-store.h"

namespace redis_core::redis_command
{

// Owns all transaction state (MULTI/EXEC/DISCARD queues and WATCH/dirty
// bookkeeping) previously embedded in RedisExecutor. Implements
// ITransactionService so commands can reach it through CommandDeps.
class TransactionManager : public ITransactionService
{
public:
  TransactionManager(redis_storage::RedisStorePtr store,
                     IBlockingService &blocking,
                     IReplicationService &replication);

  ExecutionOutcome run_multi(std::span<std::string const> args,
                             CommandContext const &ctx) override;
  ExecutionOutcome run_exec(std::span<std::string const> args,
                            CommandContext const &ctx) override;
  ExecutionOutcome run_discard(std::span<std::string const> args,
                               CommandContext const &ctx) override;
  ExecutionOutcome run_watch(std::span<std::string const> args,
                             CommandContext const &ctx) override;
  ExecutionOutcome run_unwatch(std::span<std::string const> args,
                               CommandContext const &ctx) override;

  [[nodiscard]] bool is_in_transaction(int client_fd) const;
  void queue_command(ICommand *command,
                     std::vector<std::string> args,
                     CommandContext ctx);

  void mark_watched_key_dirty(std::string const &key);
  void on_close_clean_up(int fd);

private:
  struct TransactionCommand
  {
    TransactionCommand(ICommand *command,
                       std::vector<std::string> args,
                       CommandContext ctx) :
        command{command}, args{std::move(args)}, ctx(std::move(ctx))
    {
    }
    ICommand *command;
    std::vector<std::string> args;
    CommandContext ctx;
  };

  void clear_watched_keys(int client_fd);

  redis_storage::RedisStorePtr p_store_;
  IBlockingService &blocking_;
  IReplicationService &replication_;

  std::unordered_map<int, std::queue<TransactionCommand>>
          clients_transaction_queue_;
  std::unordered_map<std::string, std::unordered_set<int>>
          watched_clients_by_key_;
  std::unordered_map<int, std::unordered_set<std::string>>
          watched_keys_by_clients_;
  std::unordered_set<int> dirty_clients_;
};

} // namespace redis_core::redis_command

#endif // REDIS_CPP_TRANSACTION_MANAGER_H
