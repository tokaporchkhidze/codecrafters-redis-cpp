#include "incr-command.h"

namespace redis_core::redis_command
{

ArgSpec IncrCommand::arg_spec() const { return {1, 1}; }

ExecutionOutcome IncrCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &deps)
{
  auto const res{deps.store->incr(args[0])};
  if (res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, Integer{res.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY, SimpleError{res.error()}};
}

} // namespace redis_core::redis_command
