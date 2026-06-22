#include "xrange-command.h"

#include "reply-builder.h"

namespace redis_core::redis_command
{

ArgSpec XrangeCommand::arg_spec() const { return {3, 3}; }

ExecutionOutcome XrangeCommand::execute(std::span<std::string const> const args,
                                        CommandContext const &,
                                        CommandDeps &deps)
{
  auto const entries_res{deps.store->xrange(args[0], args[1], args[2])};
  if (!entries_res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError(entries_res.error())};
  }

  return ExecutionOutcome{ResultType::REPLY,
                          make_stream_entries_reply(entries_res.value())};
}

} // namespace redis_core::redis_command
