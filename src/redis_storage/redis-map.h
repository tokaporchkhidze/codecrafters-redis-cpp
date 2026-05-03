#ifndef REDIS_CPP_REDIS_MAP_H
#define REDIS_CPP_REDIS_MAP_H

#include <string>
#include <unordered_map>
#include <optional>

namespace redis_storage
{

class RedisMap
{
public:
  void set(std::string const &key, std::string const &value);
  std::optional<std::string> get(std::string const &key);
private:
  std::unordered_map<std::string, std::string> data_{};
};

} // namespace redis_storage


#endif // REDIS_CPP_REDIS_MAP_H
