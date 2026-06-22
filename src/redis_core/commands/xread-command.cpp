#include "xread-command.h"

#include "../services/blocking-service.h"

namespace redis_core::redis_command
{

ArgSpec XreadCommand::arg_spec() const { return {3, std::nullopt}; }

ExecutionOutcome XreadCommand::execute(std::span<std::string const> const args,
                                       CommandContext const &ctx,
                                       CommandDeps &deps)
{
  return deps.blocking.run_xread(args, ctx);
}

} // namespace redis_core::redis_command
