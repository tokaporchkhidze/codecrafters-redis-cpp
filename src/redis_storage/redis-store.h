#ifndef REDIS_CPP_REDIS_STORE_H
#define REDIS_CPP_REDIS_STORE_H

#include <chrono>
#include <deque>
#include <expected>
#include <memory>
#include <unordered_map>
#include <variant>


namespace redis_storage
{

class RedisStore
{
public:
  enum class StoreError
  {
    WrongType = 0,
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

private:
  using List = std::deque<std::string>;
  using ValueType = std::variant<std::string, List>;

  struct RedisValue
  {
    ValueType value;
    std::optional<std::chrono::steady_clock::time_point> expires_at;
  };

  std::unordered_map<std::string, RedisValue> map_{};
};

using RedisStorePtr = std::shared_ptr<RedisStore>;

} // namespace redis_storage


#endif // REDIS_CPP_REDIS_STORE_H
