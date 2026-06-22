#ifndef REDIS_CPP_TRANSACTION_SERVICE_H
#define REDIS_CPP_TRANSACTION_SERVICE_H

#include <span>
#include <string>

#include "../command-context.h"
#include "../commands/command-interface.h"

namespace redis_core::redis_command
{

// Focused collaborator exposed to commands instead of the whole executor.
// Backed by the dedicated TransactionManager class that RedisExecutor owns.
class ITransactionService
{
public:
  virtual ~ITransactionService() = default;

  virtual ExecutionOutcome run_multi(std::span<std::string const> args,
                                     CommandContext const& ctx) = 0;
  virtual ExecutionOutcome run_exec(std::span<std::string const> args,
                                    CommandContext const& ctx) = 0;
  virtual ExecutionOutcome run_discard(std::span<std::string const> args,
                                       CommandContext const& ctx) = 0;
  virtual ExecutionOutcome run_watch(std::span<std::string const> args,
                                     CommandContext const& ctx) = 0;
  virtual ExecutionOutcome run_unwatch(std::span<std::string const> args,
                                       CommandContext const& ctx) = 0;
};

} // namespace redis_core::redis_command

#endif // REDIS_CPP_TRANSACTION_SERVICE_H
