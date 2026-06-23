#ifndef REDIS_CPP_COMMAND_REGISTRY_H
#define REDIS_CPP_COMMAND_REGISTRY_H

#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "command-interface.h"

namespace redis_core::redis_command
{

// Owns the name -> command mapping and the lookup machinery.
// Its only responsibility is registration and (transparent) lookup.
class CommandRegistry
{
public:
  void register_command(std::string name, CommandPtr command)
  {
    registry_.insert_or_assign(std::move(name), std::move(command));
  }

  // TODO: I don' like using raw pointer here.
  // Need to address this.
  [[nodiscard]] ICommand *find(std::string_view const name) const
  {
    auto const it{registry_.find(name)};
    return it == registry_.end() ? nullptr : it->second.get();
  }

private:
  // For enabling lookup of commands with string_view-s.
  struct StringHash
  {
    using is_transparent = void;

    std::size_t operator()(std::string_view const value) const
    {
      return std::hash<std::string_view>{}(value);
    }
  };

  std::unordered_map<std::string, CommandPtr, StringHash, std::equal_to<>>
          registry_;
};

// Registers all built-in commands. Adding a new command is a single line here
// plus the command class itself; the dispatcher never changes.
void register_builtin_commands(CommandRegistry &registry);

} // namespace redis_core::redis_command

#endif // REDIS_CPP_COMMAND_REGISTRY_H
