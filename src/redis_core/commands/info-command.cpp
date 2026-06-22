#include "info-command.h"

#include <algorithm>
#include <cctype>
#include <format>
#include <string_view>

namespace redis_core::redis_command
{

namespace
{

bool command_arg_equals(std::string_view const lhs, std::string_view const rhs)
{
  return std::ranges::equal(
          lhs, rhs, [](char const a, char const b)
          { return std::toupper(a) == std::toupper(b); });
}

} // namespace

ArgSpec InfoCommand::arg_spec() const { return {0, 1}; }

ExecutionOutcome InfoCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &deps)
{
  if (command_arg_equals(args[0], "replication")) {
    return ExecutionOutcome{
            ResultType::REPLY,
            BulkString(std::format("{}\nrole:{}",
                                   "# Replication",
                                   deps.is_master ? "master" : "slave"))};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Unsupported info argument")};
}

} // namespace redis_core::redis_command
