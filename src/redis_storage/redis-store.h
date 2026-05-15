#ifndef REDIS_CPP_REDIS_STORE_H
#define REDIS_CPP_REDIS_STORE_H

#include <chrono>
#include <deque>
#include <expected>
#include <memory>
#include <unordered_map>
#include <variant>
#include "redis-stream.h"


namespace redis_storage
{

class RedisStore
{
public:
  enum class StoreError
  {
    WRONG_TYPE = 0,
    KEY_NOT_FOUND,
  };

  struct SetOptions
  {
    std::optional<std::chrono::milliseconds> ttl_ms;
  };

  void set(std::string const &key,
           std::string const &value,
           SetOptions const &options = {});
  std::optional<std::string> get(std::string const &key);

  std::expected<int64_t, StoreError> rpush(std::string const &key,
                                           std::span<std::string const> values);

  std::expected<int64_t, StoreError> lpush(std::string const &key,
                                           std::span<std::string const> values);

  std::expected<int64_t, StoreError> llen(std::string const &key);

  std::vector<std::string>
  lrange(std::string const &key, int64_t start, int64_t stop);

  std::expected<std::vector<std::string>, StoreError>
  lpop(std::string const &key, int64_t count);

  std::string get_type(std::string const &key);

  std::expected<std::string, std::string>
  xadd(std::string const &key,
       std::span<std::pair<std::string, std::string> const> fields,
       std::string const &requested_id);

  std::expected<std::vector<StreamEntry>, std::string> xrange(std::string const &key,
                                                 std::string const &start,
                                                 std::string const &end);

private:
  using List = std::deque<std::string>;
  using ValueType = std::variant<std::string, List, RedisStream>;

  struct RedisValue
  {
    ValueType value;
    std::optional<std::chrono::steady_clock::time_point> expires_at;
  };

  std::unordered_map<std::string, RedisValue> map_{};

  enum class PushSide
  {
    LEFT,
    RIGHT,
  };

  std::expected<std::reference_wrapper<List>, StoreError>
  get_or_create_list(std::string const &key);

  template<PushSide side>
  std::expected<int64_t, StoreError>
  push_to_list(std::string const &key,
               std::span<std::string const> const values)
  {
    auto list = get_or_create_list(key);
    if (!list.has_value()) {
      return std::unexpected(list.error());
    }

    auto &items = list->get();

    for (auto const &new_value: values) {
      if constexpr (side == PushSide::RIGHT) {
        items.push_back(new_value);
      } else {
        items.push_front(new_value);
      }
    }

    return static_cast<int64_t>(items.size());
  }

  std::expected<std::reference_wrapper<RedisStream>, StoreError>
  get_or_create_stream(std::string const &key);
};

using RedisStorePtr = std::shared_ptr<RedisStore>;

} // namespace redis_storage


#endif // REDIS_CPP_REDIS_STORE_H
