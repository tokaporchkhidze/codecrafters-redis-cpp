#include "redis-executor.h"

#include <climits>
#include <format>
#include <utility>

#include "reply-builder.h"

using namespace redis_core;
using namespace redis_storage;
using namespace redis_core::redis_command;

RedisExecutor::RedisExecutor(RedisStorePtr p_redis_store,
                             ReplicationManagerPtr p_replication) :
    p_store_(std::move(p_redis_store)),
    p_replication_(std::move(p_replication)),
    blocking_manager_(
            p_store_,
            [this](int const client_fd)
            { return transaction_manager_.is_in_transaction(client_fd); }),
    transaction_manager_(p_store_, blocking_manager_, *p_replication_)
{
  register_builtin_commands(registry_);

  p_store_->set_key_modified_callback(
          [this](std::string const &key)
          { transaction_manager_.mark_watched_key_dirty(key); });
}

RedisExecutor::ExecutionResult RedisExecutor::execute(RedisCommand &cmd,
                                                      CommandContext const &ctx)
{
  auto &args{cmd.args()};

  auto *const command{registry_.find(cmd.name())};
  if (command == nullptr) {
    return ExecutionResult{ResultType::REPLY,
                           encode_reply(SimpleError("Unknown command"))};
  }

  auto const spec{command->arg_spec()};
  if (args.size() < spec.min_argc ||
      args.size() > spec.max_argc.value_or(INT_MAX)) {
    return ExecutionResult{
            ResultType::REPLY,
            encode_reply(SimpleError(std::format(
                    "Invalid number of arguments - {}", args.size())))};
  }

  if (transaction_manager_.is_in_transaction(ctx.client_fd) &&
      command->transaction_policy() == TransactionPolicy::QUEUE) {
    transaction_manager_.queue_command(command, std::move(args), ctx);
    return ExecutionResult{ResultType::REPLY,
                           encode_reply(SimpleString("QUEUED"))};
  }

  CommandDeps deps{
          p_store_, blocking_manager_, transaction_manager_, *p_replication_};
  auto [type, reply]{command->execute(args, ctx, deps)};
  return ExecutionResult{type, encode_reply(reply)};
}

void RedisExecutor::on_close_clean_up(int const fd)
{
  blocking_manager_.on_close_clean_up(fd);
  transaction_manager_.on_close_clean_up(fd);
}

void RedisExecutor::expire_blocked_clients(
        std::chrono::steady_clock::time_point const now)
{
  blocking_manager_.expire_blocked_clients(now);
}

std::optional<std::chrono::steady_clock::time_point>
RedisExecutor::get_next_blocked_client_timeout()
{
  return blocking_manager_.get_next_blocked_client_timeout();
}

bool RedisExecutor::is_in_transaction(int const client_fd) const
{
  return transaction_manager_.is_in_transaction(client_fd);
}
