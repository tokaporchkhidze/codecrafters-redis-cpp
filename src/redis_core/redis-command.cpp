#include "redis-command.h"

#include <algorithm>
#include <ranges>
#include <stdexcept>

using namespace redis_core;

RedisCommand::RedisCommand(std::vector<std::string> args)
{
  if (args.empty()) {
    throw std::invalid_argument{"empty name"};
  }
  command_ = std::move(args.front());
  // Normalize the name case to upper.
  std::ranges::transform(command_,
                         command_.begin(),
                         [](unsigned char const c)
                         { return static_cast<char>(std::toupper(c)); });

  args_.assign(std::make_move_iterator(std::next(args.begin())),
               std::make_move_iterator(args.end()));
}

std::string_view RedisCommand::name() const { return command_; }

std::vector<std::string> const &RedisCommand::args() const { return args_; }

std::vector<std::string> RedisCommand::args() { return args_; }
