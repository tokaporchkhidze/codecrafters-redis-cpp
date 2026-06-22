#include "watch-command.h"

#include "../services/transaction-service.h"

namespace redis_core::redis_command
{

ArgSpec WatchCommand::arg_spec() const { return {1, std::nullopt}; }

TransactionPolicy WatchCommand::transaction_policy() const
{
  return TransactionPolicy::EXECUTE_IMMEDIATELY;
}

ExecutionOutcome WatchCommand::execute(std::span<std::string const> const args,
                                       CommandContext const &ctx,
                                       CommandDeps &deps)
{
  return deps.transactions.run_watch(args, ctx);
}

} // namespace redis_core::redis_command
