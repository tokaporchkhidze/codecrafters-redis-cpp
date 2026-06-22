#include "llen-command.h"

namespace redis_core::redis_command
{

ArgSpec LlenCommand::arg_spec() const { return {1, 1}; }

ExecutionOutcome LlenCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &deps)
{
  if (auto const size{deps.store->llen(args[0])}; size.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, Integer{size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to get the list length")};
}

} // namespace redis_core::redis_command
