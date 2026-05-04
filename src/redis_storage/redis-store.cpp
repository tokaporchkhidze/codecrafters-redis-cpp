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
    auto &[value, expires_at] = it->second;
    if (expires_at.has_value() &&
        expires_at.value() < std::chrono::steady_clock::now()) {
      map_.erase(it);
      return std::nullopt;
    }
    // TODO: This is incorrect, assumes value type always string.
    return std::get<std::string>(value);
  }
  return std::nullopt;
}

std::expected<int64_t, RedisStore::StoreError>
RedisStore::rpush(std::string const &key,
                  std::span<std::string const> const values)
{
  auto [it, inserted] = map_.try_emplace(key,
                                         RedisValue{
                                                 List{},
                                                 std::nullopt,
                                         });

  if (!std::holds_alternative<List>(it->second.value)) {
    if (it->second.expires_at.has_value() &&
        it->second.expires_at.value() < std::chrono::steady_clock::now()) {
      map_.erase(it);
      it = map_.try_emplace(key,
                            RedisValue{
                                    List{},
                                    std::nullopt,
                            })
                   .first;
    } else {
      return std::unexpected(StoreError::WrongType);
    }
  }

  auto &list = std::get<List>(it->second.value);
  for (auto const &new_value: values) {
    list.push_back(new_value);
  }

  return list.size();
}
