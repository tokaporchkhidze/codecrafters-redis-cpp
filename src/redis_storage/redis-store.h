#ifndef REDIS_CPP_REDIS_STORE_H
#define REDIS_CPP_REDIS_STORE_H

#include <memory>


#include "redis-map.h"

namespace redis_storage
{

class RedisStore
{
public:
  void set(std::string const &key, std::string const &value);
  std::optional<std::string> get(std::string const &key);

private:
  RedisMap map_{};
};

using RedisStorePtr = std::shared_ptr<RedisStore>;

} // namespace redis_storage


#endif // REDIS_CPP_REDIS_STORE_H
