#include "info-command.h"

#include <format>

#include "../command-arg-utils.h"
#include "../services/replication-service.h"

namespace redis_core::redis_command
{

ArgSpec InfoCommand::arg_spec() const { return {0, 1}; }

ExecutionOutcome InfoCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &deps)
{
  if (command_arg_equals(args[0], "replication")) {
    auto const &replication{deps.replication};
    return ExecutionOutcome{
            ResultType::REPLY,
            BulkString(std::format("# Replication\r\n"
                                   "role:{}\r\n"
                                   "master_replid:{}\r\n"
                                   "master_repl_offset:{}",
                                   replication.is_master() ? "master" : "slave",
                                   replication.replid(),
                                   replication.repl_offset()))};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Unsupported info argument")};
}

} // namespace redis_core::redis_command
