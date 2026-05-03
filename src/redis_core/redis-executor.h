#ifndef REDIS_CPP_REDIS_EXECUTOR_H
#define REDIS_CPP_REDIS_EXECUTOR_H

#include <string>

#include "redis-command.h"
#include "redis_storage/redis-store.h"

using namespace redis_storage;

namespace redis_core
{

class RedisExecutor
{
public:
  explicit RedisExecutor(RedisStorePtr p_redis_store);

  std::string execute(RedisCommand const& cmd);

private:
  RedisStorePtr p_redis_store_;
};

using RedisExecutorPtr = std::shared_ptr<RedisExecutor>;

} // namespace redis_core

#endif // REDIS_CPP_REDIS_EXECUTOR_H
