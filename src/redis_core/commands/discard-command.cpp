#include "discard-command.h"

#include "../services/transaction-service.h"

namespace redis_core::redis_command
{

ArgSpec DiscardCommand::arg_spec() const { return {0, 0}; }

TransactionPolicy DiscardCommand::transaction_policy() const
{
  return TransactionPolicy::EXECUTE_IMMEDIATELY;
}

ExecutionOutcome
DiscardCommand::execute(std::span<std::string const> const args,
                        CommandContext const &ctx,
                        CommandDeps &deps)
{
  return deps.transactions.run_discard(args, ctx);
}

} // namespace redis_core::redis_command
