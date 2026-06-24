#include "command-registry.h"

#include <memory>

#include "blpop-command.h"
#include "discard-command.h"
#include "echo-command.h"
#include "exec-command.h"
#include "get-command.h"
#include "incr-command.h"
#include "info-command.h"
#include "llen-command.h"
#include "lpop-command.h"
#include "lpush-command.h"
#include "lrange-command.h"
#include "multi-command.h"
#include "ping-command.h"
#include "repl-conf-command.h"
#include "rpush-command.h"
#include "set-command.h"
#include "type-command.h"
#include "unwatch-command.h"
#include "watch-command.h"
#include "xadd-command.h"
#include "xrange-command.h"
#include "xread-command.h"

namespace redis_core::redis_command
{

void register_builtin_commands(CommandRegistry &registry)
{
  registry.register_command("PING", std::make_unique<PingCommand>());
  registry.register_command("ECHO", std::make_unique<EchoCommand>());
  registry.register_command("GET", std::make_unique<GetCommand>());
  registry.register_command("SET", std::make_unique<SetCommand>());
  registry.register_command("INCR", std::make_unique<IncrCommand>());
  registry.register_command("TYPE", std::make_unique<TypeCommand>());
  registry.register_command("LLEN", std::make_unique<LlenCommand>());
  registry.register_command("LRANGE", std::make_unique<LrangeCommand>());
  registry.register_command("LPOP", std::make_unique<LpopCommand>());
  registry.register_command("RPUSH", std::make_unique<RpushCommand>());
  registry.register_command("LPUSH", std::make_unique<LpushCommand>());
  registry.register_command("BLPOP", std::make_unique<BlpopCommand>());
  registry.register_command("XADD", std::make_unique<XaddCommand>());
  registry.register_command("XRANGE", std::make_unique<XrangeCommand>());
  registry.register_command("XREAD", std::make_unique<XreadCommand>());
  registry.register_command("MULTI", std::make_unique<MultiCommand>());
  registry.register_command("EXEC", std::make_unique<ExecCommand>());
  registry.register_command("DISCARD", std::make_unique<DiscardCommand>());
  registry.register_command("WATCH", std::make_unique<WatchCommand>());
  registry.register_command("UNWATCH", std::make_unique<UnwatchCommand>());
  registry.register_command("INFO", std::make_unique<InfoCommand>());
  registry.register_command("REPLCONF", std::make_unique<ReplConfCommand>());
}

} // namespace redis_core::redis_command
