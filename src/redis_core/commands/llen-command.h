#ifndef REDIS_CPP_LLEN_COMMAND_H
#define REDIS_CPP_LLEN_COMMAND_H

#include <span>
#include <string>

#include "command-interface.h"

namespace redis_core::redis_command
{

class LlenCommand final : public ICommand
{
public:
  [[nodiscard]] ArgSpec arg_spec() const override;

  ExecutionOutcome execute(std::span<std::string const> args,
                           CommandContext const &ctx,
                           CommandDeps &deps) override;
};

} // namespace redis_core::redis_command

#endif // REDIS_CPP_LLEN_COMMAND_H
