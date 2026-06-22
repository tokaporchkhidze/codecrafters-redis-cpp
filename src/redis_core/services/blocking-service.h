#ifndef REDIS_CPP_BLOCKING_SERVICE_H
#define REDIS_CPP_BLOCKING_SERVICE_H

#include <span>
#include <string>

#include "../command-context.h"
#include "../commands/command-interface.h"

namespace redis_core::redis_command
{

class IBlockingService
{
public:
  virtual ~IBlockingService() = default;

  virtual void unblock_client_for_key(std::string const& key) = 0;
  virtual ExecutionOutcome run_blpop(std::span<std::string const> args,
                                     CommandContext const& ctx) = 0;
  virtual ExecutionOutcome run_xread(std::span<std::string const> args,
                                     CommandContext const& ctx) = 0;
};

} // namespace redis_core::redis_command

#endif // REDIS_CPP_BLOCKING_SERVICE_H
