#include "exec-command.h"

#include "../services/transaction-service.h"

namespace redis_core::redis_command
{

ArgSpec ExecCommand::arg_spec() const { return {0, 0}; }

TransactionPolicy ExecCommand::transaction_policy() const
{
  return TransactionPolicy::EXECUTE_IMMEDIATELY;
}

ExecutionOutcome ExecCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &ctx,
                                      CommandDeps &deps)
{
  return deps.transactions.run_exec(args, ctx);
}

} // namespace redis_core::redis_command
