#include "get-command.h"

namespace redis_core::redis_command
{

ArgSpec GetCommand::arg_spec() const { return {1, 1}; }

ExecutionOutcome GetCommand::execute(std::span<std::string const> const args,
                                     CommandContext const &,
                                     CommandDeps &deps)
{
  if (auto const value{deps.store->get(args[0])}; value.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, BulkString(value.value())};
  }
  return ExecutionOutcome{ResultType::REPLY, NullBulkString{}};
}

} // namespace redis_core::redis_command
