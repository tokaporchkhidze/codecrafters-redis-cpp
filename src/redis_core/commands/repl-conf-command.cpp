#include "repl-conf-command.h"

#include "redis_core/command-arg-utils.h"

using namespace redis_core::redis_command;

ArgSpec ReplConfCommand::arg_spec() const { return {2, 2}; }

ExecutionOutcome
ReplConfCommand::execute(std::span<std::string const> const args,
                         const CommandContext &ctx,
                         CommandDeps &deps)
{
  return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
}
