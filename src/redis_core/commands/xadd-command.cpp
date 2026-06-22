#include "xadd-command.h"

#include <utility>
#include <vector>

#include "../services/blocking-service.h"

namespace redis_core::redis_command
{

ArgSpec XaddCommand::arg_spec() const { return {4, std::nullopt}; }

ExecutionOutcome XaddCommand::execute(std::span<std::string const> const args,
                                      CommandContext const &,
                                      CommandDeps &deps)
{
  std::vector<std::pair<std::string, std::string>> fields;
  if (args.size() % 2 != 0) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError("Invalid number of arguments")};
  }
  for (auto i = 2; i < args.size(); i += 2) {
    fields.emplace_back(args[i], args[i + 1]);
  }
  if (auto const id{deps.store->xadd(args[0], fields, args[1])};
      id.has_value()) {
    deps.blocking.unblock_client_for_key(args[0]);
    return ExecutionOutcome{ResultType::REPLY, BulkString(id.value())};
  } else {
    return ExecutionOutcome{ResultType::REPLY, SimpleError(id.error())};
  }
}

} // namespace redis_core::redis_command
