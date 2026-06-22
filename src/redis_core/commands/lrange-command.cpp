#include "lrange-command.h"

#include <ranges>
#include <vector>

namespace redis_core::redis_command
{

ArgSpec LrangeCommand::arg_spec() const { return {3, 3}; }

ExecutionOutcome LrangeCommand::execute(std::span<std::string const> const args,
                                        CommandContext const &,
                                        CommandDeps &deps)
{
  auto elements{deps.store->lrange(
          args[0], std::stoll(args[1]), std::stoll(args[2]))};
  auto values = elements |
                std::views::transform(
                        [](std::string &element)
                        { return RedisReply(BulkString(std::move(element))); });

  return ExecutionOutcome{
          ResultType::REPLY,
          Array(std::ranges::to<std::vector<RedisReply>>(values))};
}

} // namespace redis_core::redis_command
