#include "echo-command.h"

namespace redis_core::redis_command
{

ArgSpec EchoCommand::arg_spec() const { return {0, 1}; }

ExecutionOutcome EchoCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &)
{
  if (args.empty()) {
    return ExecutionOutcome{ResultType::REPLY, BulkString("")};
  }
  return ExecutionOutcome{ResultType::REPLY, BulkString(args[0])};
}

} // namespace redis_core::redis_command
