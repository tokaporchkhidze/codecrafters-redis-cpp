#include "rpush-command.h"

#include "../services/blocking-service.h"

namespace redis_core::redis_command
{

ArgSpec RpushCommand::arg_spec() const { return {2, std::nullopt}; }

ExecutionOutcome RpushCommand::execute(std::span<std::string const> const args,
                                       CommandContext const &,
                                       CommandDeps &deps)
{
  if (auto const new_size{deps.store->rpush(args[0], args.subspan(1))};
      new_size.has_value()) {
    deps.blocking.unblock_client_for_key(args[0]);
    return ExecutionOutcome{ResultType::REPLY, Integer{new_size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to push to the list")};
}

} // namespace redis_core::redis_command
