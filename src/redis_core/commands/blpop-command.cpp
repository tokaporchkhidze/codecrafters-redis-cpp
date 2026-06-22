#include "blpop-command.h"

#include "../services/blocking-service.h"

namespace redis_core::redis_command
{

ArgSpec BlpopCommand::arg_spec() const { return {2, std::nullopt}; }

ExecutionOutcome BlpopCommand::execute(std::span<std::string const> const args,
                                       CommandContext const &ctx,
                                       CommandDeps &deps)
{
  return deps.blocking.run_blpop(args, ctx);
}

} // namespace redis_core::redis_command
