#include "multi-command.h"

#include "../services/transaction-service.h"

namespace redis_core::redis_command
{

ArgSpec MultiCommand::arg_spec() const { return {0, 0}; }

ExecutionOutcome MultiCommand::execute(std::span<std::string const> const args,
                                       CommandContext const &ctx,
                                       CommandDeps &deps)
{
  return deps.transactions.run_multi(args, ctx);
}

} // namespace redis_core::redis_command
