#include <memory>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

#include "redis_core/command-context.h"
#include "redis_core/commands/command-interface.h"
#include "redis_core/commands/command-registry.h"
#include "redis_core/services/blocking-manager.h"
#include "redis_core/services/replication-manager.h"
#include "redis_core/services/transaction-manager.h"
#include "redis_core/commands/blpop-command.h"
#include "redis_core/commands/discard-command.h"
#include "redis_core/commands/echo-command.h"
#include "redis_core/commands/exec-command.h"
#include "redis_core/commands/get-command.h"
#include "redis_core/commands/info-command.h"
#include "redis_core/commands/lpush-command.h"
#include "redis_core/commands/multi-command.h"
#include "redis_core/commands/rpush-command.h"
#include "redis_core/commands/watch-command.h"
#include "redis_core/commands/xadd-command.h"
#include "redis_core/commands/incr-command.h"
#include "redis_core/commands/llen-command.h"
#include "redis_core/commands/lpop-command.h"
#include "redis_core/commands/lrange-command.h"
#include "redis_core/commands/ping-command.h"
#include "redis_core/commands/set-command.h"
#include "redis_core/commands/type-command.h"
#include "redis_storage/redis-store.h"

using namespace redis_core;
using namespace redis_core::redis_command;

namespace
{

CommandContext make_context(int const fd = 1)
{
  return CommandContext{fd, [](std::string) {}};
}

// Wires up the real focused collaborators (BlockingManager / TransactionManager)
// that commands depend on through CommandDeps.
struct TestServices
{
  redis_storage::RedisStorePtr store;
  BlockingManager blocking;
  ReplicationManager replication;
  TransactionManager transactions;

  explicit TestServices(bool const is_master = true) :
      store(std::make_shared<redis_storage::RedisStore>()),
      blocking(store,
               [this](int const fd)
               { return transactions.is_in_transaction(fd); }),
      replication(is_master ? std::nullopt
                            : std::optional<std::string>{"localhost"},
                  is_master ? std::nullopt : std::optional<int>{6379}),
      transactions(store, blocking, replication)
  {
  }
};

std::string const &as_simple_string(RedisReply const &reply)
{
  return std::get<SimpleString>(reply.value).value;
}

std::string const &as_bulk_string(RedisReply const &reply)
{
  return std::get<BulkString>(reply.value).value;
}

} // namespace

TEST(PingCommandTest, ReportsZeroArgSpec)
{
  PingCommand const command;
  auto const spec{command.arg_spec()};

  EXPECT_EQ(spec.min_argc, 0);
  ASSERT_TRUE(spec.max_argc.has_value());
  EXPECT_EQ(spec.max_argc.value(), 0);
}

TEST(PingCommandTest, DefaultsToQueueTransactionPolicy)
{
  PingCommand const command;

  EXPECT_EQ(command.transaction_policy(), TransactionPolicy::QUEUE);
}

TEST(PingCommandTest, ExecuteRepliesWithPong)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};

  PingCommand command;
  std::vector<std::string> const args{};
  auto const ctx{make_context()};

  auto const outcome{command.execute(std::span<std::string const>{args},
                                     ctx,
                                     deps)};

  EXPECT_EQ(outcome.type, ResultType::REPLY);
  EXPECT_EQ(as_simple_string(outcome.reply), "PONG");
}

TEST(CommandRegistryTest, ReturnsNullptrForUnknownCommand)
{
  CommandRegistry registry;

  EXPECT_EQ(registry.find("NOPE"), nullptr);
}

TEST(CommandRegistryTest, FindsRegisteredCommand)
{
  CommandRegistry registry;
  registry.register_command("PING", std::make_unique<PingCommand>());

  EXPECT_NE(registry.find("PING"), nullptr);
}

TEST(CommandRegistryTest, RegisterBuiltinCommandsRegistersPing)
{
  CommandRegistry registry;
  register_builtin_commands(registry);

  EXPECT_NE(registry.find("PING"), nullptr);
}

TEST(CommandRegistryTest, RegisterBuiltinCommandsRegistersStoreCommands)
{
  CommandRegistry registry;
  register_builtin_commands(registry);

  for (auto const *const name:
       {"ECHO", "GET", "SET", "INCR", "TYPE", "LLEN", "LRANGE", "LPOP"}) {
    EXPECT_NE(registry.find(name), nullptr) << name;
  }
}

TEST(CommandRegistryTest, RegisterBuiltinCommandsRegistersSeamCommands)
{
  CommandRegistry registry;
  register_builtin_commands(registry);

  for (auto const *const name: {"RPUSH",
                                "LPUSH",
                                "BLPOP",
                                "XADD",
                                "XRANGE",
                                "XREAD",
                                "MULTI",
                                "EXEC",
                                "DISCARD",
                                "WATCH",
                                "UNWATCH",
                                "INFO"}) {
    EXPECT_NE(registry.find(name), nullptr) << name;
  }
}

TEST(ExecCommandTest, UsesExecuteImmediatelyTransactionPolicy)
{
  ExecCommand const exec_command;
  DiscardCommand const discard_command;
  WatchCommand const watch_command;

  EXPECT_EQ(exec_command.transaction_policy(),
            TransactionPolicy::EXECUTE_IMMEDIATELY);
  EXPECT_EQ(discard_command.transaction_policy(),
            TransactionPolicy::EXECUTE_IMMEDIATELY);
  EXPECT_EQ(watch_command.transaction_policy(),
            TransactionPolicy::EXECUTE_IMMEDIATELY);
}

TEST(MultiCommandTest, DefaultsToQueueTransactionPolicy)
{
  MultiCommand const command;

  EXPECT_EQ(command.transaction_policy(), TransactionPolicy::QUEUE);
}

TEST(RpushCommandTest, AppendsAndReturnsListSize)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};
  auto const ctx{make_context()};

  RpushCommand command;
  std::vector<std::string> const args{"list", "a", "b"};
  auto const outcome{
          command.execute(std::span<std::string const>{args}, ctx, deps)};

  EXPECT_EQ(outcome.type, ResultType::REPLY);
  EXPECT_EQ(std::get<Integer>(outcome.reply.value).value, 2);
}

TEST(XaddCommandTest, AddsEntryAndReturnsId)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};
  auto const ctx{make_context()};

  XaddCommand command;
  std::vector<std::string> const args{"stream", "1-1", "field", "value"};
  auto const outcome{
          command.execute(std::span<std::string const>{args}, ctx, deps)};

  EXPECT_EQ(outcome.type, ResultType::REPLY);
  EXPECT_EQ(as_bulk_string(outcome.reply), "1-1");
}

TEST(MultiCommandTest, StartsTransactionAndRepliesOk)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};
  auto const ctx{make_context()};

  MultiCommand command;
  std::vector<std::string> const args{};
  auto const outcome{
          command.execute(std::span<std::string const>{args}, ctx, deps)};

  EXPECT_EQ(as_simple_string(outcome.reply), "OK");
  EXPECT_TRUE(services.transactions.is_in_transaction(ctx.client_fd));
}

TEST(InfoCommandTest, ReplicationReportsRole)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};
  auto const ctx{make_context()};

  InfoCommand command;
  std::vector<std::string> const args{"replication"};
  auto const outcome{
          command.execute(std::span<std::string const>{args}, ctx, deps)};

  EXPECT_NE(as_bulk_string(outcome.reply).find("role:master"),
            std::string::npos);
}

TEST(EchoCommandTest, EchoesArgument)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};

  EchoCommand command;
  std::vector<std::string> const args{"hello"};
  auto const ctx{make_context()};

  auto const outcome{
          command.execute(std::span<std::string const>{args}, ctx, deps)};

  EXPECT_EQ(as_bulk_string(outcome.reply), "hello");
}

TEST(SetGetCommandTest, SetThenGetReturnsValue)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};
  auto const ctx{make_context()};

  SetCommand set_command;
  std::vector<std::string> const set_args{"key", "value"};
  auto const set_outcome{
          set_command.execute(std::span<std::string const>{set_args},
                              ctx,
                              deps)};
  EXPECT_EQ(as_simple_string(set_outcome.reply), "OK");

  GetCommand get_command;
  std::vector<std::string> const get_args{"key"};
  auto const get_outcome{
          get_command.execute(std::span<std::string const>{get_args},
                              ctx,
                              deps)};
  EXPECT_EQ(as_bulk_string(get_outcome.reply), "value");
}

TEST(GetCommandTest, MissingKeyReturnsNullBulkString)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};
  auto const ctx{make_context()};

  GetCommand command;
  std::vector<std::string> const args{"absent"};
  auto const outcome{
          command.execute(std::span<std::string const>{args}, ctx, deps)};

  EXPECT_TRUE(std::holds_alternative<NullBulkString>(outcome.reply.value));
}

TEST(IncrCommandTest, IncrementsAndReturnsInteger)
{
  TestServices services;
  CommandDeps deps{services.store, services.blocking, services.transactions,
                   services.replication};
  auto const ctx{make_context()};

  IncrCommand command;
  std::vector<std::string> const args{"counter"};
  auto const outcome{
          command.execute(std::span<std::string const>{args}, ctx, deps)};

  EXPECT_EQ(std::get<Integer>(outcome.reply.value).value, 1);
}
