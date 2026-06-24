#ifndef REDIS_CPP_REPL_CONF_COMMAND_H
#define REDIS_CPP_REPL_CONF_COMMAND_H

#include "command-interface.h"

namespace redis_core::redis_command
{

class ReplConfCommand : public ICommand
{
public:
  [[nodiscard]] ArgSpec arg_spec() const override;

  ExecutionOutcome execute(std::span<std::string const> args,
                           const CommandContext &ctx,
                           CommandDeps &deps) override;
};

}



#endif // REDIS_CPP_REPL_CONF_COMMAND_H
