#include "ping-command.h"

namespace redis_core::redis_command
{

ArgSpec PingCommand::arg_spec() const { return {0, 0}; }

ExecutionOutcome PingCommand::execute(std::span<std::string const>,
                                      CommandContext const &,
                                      CommandDeps &)
{
  return ExecutionOutcome{ResultType::REPLY, SimpleString{"PONG"}};
}

} // namespace redis_core::redis_command
