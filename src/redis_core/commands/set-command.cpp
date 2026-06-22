#include "set-command.h"

#include <cctype>
#include <chrono>
#include <ranges>
#include <string>
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

ArgSpec SetCommand::arg_spec() const { return {2, 4}; }

ExecutionOutcome SetCommand::execute(std::span<std::string const> const args,
                                     CommandContext const &,
                                     CommandDeps &deps)
{
  redis_storage::RedisStore::SetOptions options;
  // TODO: Currently only supporting passive expiration.
  if (args.size() == 4 && command_arg_equals(args[2], "PX")) {
    options.ttl_ms = std::chrono::milliseconds{std::stoi(args[3])};
  }
  deps.store->set(args[0], args[1], options);
  return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
}

} // namespace redis_core::redis_command
