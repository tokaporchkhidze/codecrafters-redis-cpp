#include "redis-map.h"

using namespace redis_storage;

void RedisMap::set(std::string const &key, std::string const &value)
{
  data_.insert_or_assign(key, value);
}

std::optional<std::string> RedisMap::get(std::string const &key)
{
  if (auto const it{data_.find(key)}; it != data_.cend()) {
    return it->second;
  }
  return std::nullopt;
}
