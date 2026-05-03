#include "redis-store.h"

using namespace redis_storage;

void RedisStore::set(std::string const &key,
                     std::string const &value,
                     SetOptions const &options)
{
  std::optional<std::chrono::steady_clock::time_point> expires_at;
  if (options.ttl_ms.has_value()) {
    expires_at = std::chrono::steady_clock::now() + options.ttl_ms.value();
  }
  map_.insert_or_assign(key,
                        RedisValue{
                                value,
                                expires_at,
                        });
}

std::optional<std::string> RedisStore::get(std::string const &key)
{
  // TODO: For now just handling key-value pairs.
  if (auto const it{map_.find(key)}; it != map_.cend()) {
    auto& [value, expires_at] = it->second;
    if (expires_at.has_value() && expires_at.value() < std::chrono::steady_clock::now()) {
      map_.erase(it);
      return std::nullopt;
    }
    return value;
  }
  return std::nullopt;
}
