#ifndef REDIS_CPP_COMMAND_CONTEXT_H
#define REDIS_CPP_COMMAND_CONTEXT_H
#include <functional>
#include <string>

namespace redis_core
{

using ReplyCallback = std::function<void(std::string)>;

struct CommandContext
{
  int client_fd;
  ReplyCallback callback;
};

} // namespace redis_core

#endif // REDIS_CPP_COMMAND_CONTEXT_H
