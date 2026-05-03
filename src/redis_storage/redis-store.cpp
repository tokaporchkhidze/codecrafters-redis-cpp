#include "redis-store.h"

using namespace redis_storage;

void RedisStore::set(std::string const &key, std::string const &value)
{
  map_.set(key, value);
}

std::optional<std::string> RedisStore::get(std::string const &key)
{
  // TODO: For now just handling key-value pairs.
  return map_.get(key);
}
