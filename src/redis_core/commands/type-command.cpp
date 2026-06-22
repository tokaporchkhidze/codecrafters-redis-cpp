#include "type-command.h"

namespace redis_core::redis_command
{

ArgSpec TypeCommand::arg_spec() const { return {1, 1}; }

ExecutionOutcome TypeCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &deps)
{
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleString{deps.store->get_type(args[0])}};
}

} // namespace redis_core::redis_command
