#include "unwatch-command.h"

#include "../services/transaction-service.h"

namespace redis_core::redis_command
{

ArgSpec UnwatchCommand::arg_spec() const { return {0, 0}; }

ExecutionOutcome
UnwatchCommand::execute(std::span<std::string const> const args,
                        CommandContext const &ctx,
                        CommandDeps &deps)
{
  return deps.transactions.run_unwatch(args, ctx);
}

} // namespace redis_core::redis_command
