#ifndef REDIS_CPP_REDIS_STORE_H
#define REDIS_CPP_REDIS_STORE_H

#include <chrono>
#include <memory>
#include <unordered_map>


namespace redis_storage
{

class RedisStore
{
public:
  struct SetOptions
  {
    std::optional<std::chrono::milliseconds> ttl_ms;
  };

  void set(std::string const &key,
           std::string const &value,
           SetOptions const &options = {});
  std::optional<std::string> get(std::string const &key);

private:

  struct RedisValue
  {
    std::string value;
    std::optional<std::chrono::steady_clock::time_point> expires_at;
  };

  std::unordered_map<std::string, RedisValue> map_{};
};

using RedisStorePtr = std::shared_ptr<RedisStore>;

} // namespace redis_storage


#endif // REDIS_CPP_REDIS_STORE_H
