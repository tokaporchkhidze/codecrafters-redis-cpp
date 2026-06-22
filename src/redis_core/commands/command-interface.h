#ifndef REDIS_CPP_COMMAND_INTERFACE_H
#define REDIS_CPP_COMMAND_INTERFACE_H

#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "../command-context.h"
#include "redis_storage/redis-store.h"

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

enum class ResultType
{
  REPLY = 0,
  BLOCKED,
};

struct ExecutionOutcome
{
  ResultType type{};
  RedisReply reply;
};

class IBlockingService;
class ITransactionService;

struct CommandDeps
{
  redis_storage::RedisStorePtr store;
  IBlockingService& blocking;
  ITransactionService& transactions;
  bool is_master{};
};

enum class TransactionPolicy
{
  QUEUE,
  EXECUTE_IMMEDIATELY,
};

struct ArgSpec
{
  int min_argc{};
  std::optional<int> max_argc{};
};

class ICommand
{
public:
  virtual ~ICommand() = default;

  [[nodiscard]] virtual ArgSpec arg_spec() const = 0;
  [[nodiscard]] virtual TransactionPolicy transaction_policy() const
  {
    return TransactionPolicy::QUEUE;
  }

  virtual ExecutionOutcome execute(std::span<std::string const> args,
                                   CommandContext const& ctx,
                                   CommandDeps& deps) = 0;
};

using CommandPtr = std::unique_ptr<ICommand>;

} // namespace redis_core::redis_command

#endif // REDIS_CPP_COMMAND_INTERFACE_H