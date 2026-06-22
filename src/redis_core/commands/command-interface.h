#ifndef REDIS_CPP_COMMAND_INTERFACE_H
#define REDIS_CPP_COMMAND_INTERFACE_H

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "../command-context.h"
#include "../redis-reply.h"
#include "redis_storage/redis-store.h"

namespace redis_core::redis_command
{

enum class ResultType
{
  REPLY = 0,
  BLOCKED,
};

struct ExecutionOutcome
{
  ResultType type{};
  RedisReply reply;
};

class IBlockingService;
class ITransactionService;

struct CommandDeps
{
  redis_storage::RedisStorePtr store;
  IBlockingService& blocking;
  ITransactionService& transactions;
  bool is_master{};
};

enum class TransactionPolicy
{
  QUEUE,
  EXECUTE_IMMEDIATELY,
};

struct ArgSpec
{
  int min_argc{};
  std::optional<int> max_argc{};
};

class ICommand
{
public:
  virtual ~ICommand() = default;

  [[nodiscard]] virtual ArgSpec arg_spec() const = 0;
  [[nodiscard]] virtual TransactionPolicy transaction_policy() const
  {
    return TransactionPolicy::QUEUE;
  }

  virtual ExecutionOutcome execute(std::span<std::string const> args,
                                   CommandContext const& ctx,
                                   CommandDeps& deps) = 0;
};

using CommandPtr = std::unique_ptr<ICommand>;

} // namespace redis_core::redis_command

#endif // REDIS_CPP_COMMAND_INTERFACE_H