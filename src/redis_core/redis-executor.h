#ifndef REDIS_CPP_REDIS_EXECUTOR_H
#define REDIS_CPP_REDIS_EXECUTOR_H

#include <chrono>
#include <memory>
#include <optional>
#include <string>

#include "redis-command.h"
#include "redis_storage/redis-store.h"
#include "services/blocking-manager.h"
#include "commands/command-interface.h"
#include "commands/command-registry.h"
#include "services/transaction-manager.h"

namespace redis_core
{

class RedisExecutor
{
public:
  using ResultType = redis_command::ResultType;
  using ReplyCallback = redis_core::ReplyCallback;
  using CommandContext = redis_core::CommandContext;

  struct ExecutionResult
  {
    ResultType type;
    std::string reply;
  };

  RedisExecutor(redis_storage::RedisStorePtr p_redis_store, bool is_master);

  ExecutionResult execute(RedisCommand &cmd, CommandContext const &ctx);

  void on_close_clean_up(int fd);

  void expire_blocked_clients(std::chrono::steady_clock::time_point now);

  std::optional<std::chrono::steady_clock::time_point>
  get_next_blocked_client_timeout();

  bool is_in_transaction(int client_fd) const;

private:
  redis_storage::RedisStorePtr p_store_;
  redis_command::CommandRegistry registry_;
  bool is_master_{};
  redis_command::BlockingManager blocking_manager_;
  redis_command::TransactionManager transaction_manager_;
};

using RedisExecutorPtr = std::shared_ptr<RedisExecutor>;

} // namespace redis_core

#endif // REDIS_CPP_REDIS_EXECUTOR_H
