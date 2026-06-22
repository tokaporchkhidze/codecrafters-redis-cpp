#ifndef REDIS_CPP_REDIS_REPLY_H
#define REDIS_CPP_REDIS_REPLY_H

#include <concepts>
#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace redis_core::redis_command
{

struct SimpleString
{
  std::string value;
};

struct BulkString
{
  std::string value;
};

struct SimpleError
{
  std::string value;
};

struct NullBulkString
{
};

struct Integer
{
  int64_t value;
};

struct RedisReply;

struct Array
{
  std::vector<RedisReply> values;
};

struct NullArray
{
};

struct RedisReply
{
  using Value = std::variant<std::monostate,
                             SimpleString,
                             BulkString,
                             SimpleError,
                             Integer,
                             NullBulkString,
                             Array,
                             NullArray>;

  Value value;

  template<class T>
    requires(!std::same_as<std::remove_cvref_t<T>, RedisReply>)
  RedisReply(T reply) : value(std::move(reply))
  {
  }
};

} // namespace redis_core::redis_command

#endif // REDIS_CPP_REDIS_REPLY_H
