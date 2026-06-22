#include "lpop-command.h"

#include <cstdint>
#include <ranges>
#include <vector>

namespace redis_core::redis_command
{

ArgSpec LpopCommand::arg_spec() const { return {1, 2}; }

ExecutionOutcome LpopCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &deps)
{
  bool const has_count_arg = args.size() == 2;
  int64_t const count = has_count_arg ? std::stoll(args[1]) : 1;
  if (count < 0) {
    return ExecutionOutcome{
            ResultType::REPLY,
            SimpleError("value is out of range, must be positive")};
  }
  auto popped = deps.store->lpop(args[0], count);
  if (!popped.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, SimpleError("Wrong type")};
  }

  auto elements = std::move(popped.value());
  if (elements.empty()) {
    return ExecutionOutcome{ResultType::REPLY, NullBulkString{}};
  }

  if (has_count_arg) {
    auto values =
            elements |
            std::views::transform(
                    [](std::string &element)
                    { return RedisReply(BulkString(std::move(element))); });
    return ExecutionOutcome{
            ResultType::REPLY,
            Array(std::ranges::to<std::vector<RedisReply>>(values))};
  }

  return ExecutionOutcome{ResultType::REPLY,
                          BulkString(std::move(elements.front()))};
}

} // namespace redis_core::redis_command
