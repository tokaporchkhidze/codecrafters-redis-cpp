#ifndef REDIS_CPP_REDIS_COMMAND_H
#define REDIS_CPP_REDIS_COMMAND_H

#include <string>
#include <vector>

namespace redis_core
{

class RedisCommand
{
public:
  explicit RedisCommand(std::vector<std::string> args);

  [[nodiscard]]std::string_view name() const;
  [[nodiscard]]std::vector<std::string> const &args() const;
  [[nodiscard]]std::vector<std::string> args();

private:
  std::vector<std::string> args_;
  std::string command_;
};

} // namespace redis_core


#endif // REDIS_CPP_REDIS_COMMAND_H
