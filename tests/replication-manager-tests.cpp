#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "redis_core/services/replication-manager.h"

using namespace redis_core::redis_command;

namespace
{

ReplicationManager make_master()
{
  return ReplicationManager{std::nullopt, std::nullopt};
}

ReplicationManager make_replica()
{
  return ReplicationManager{std::optional<std::string>{"localhost"},
                            std::optional<int>{6379}};
}

bool is_lower_hex_40(std::string_view const value)
{
  if (value.size() != 40U) {
    return false;
  }
  for (auto const c: value) {
    bool const is_digit{c >= '0' && c <= '9'};
    bool const is_lower_hex{c >= 'a' && c <= 'f'};
    if (!is_digit && !is_lower_hex) {
      return false;
    }
  }
  return true;
}

} // namespace

TEST(ReplicationManagerTest, DefaultsToMasterWhenNoMasterHost)
{
  auto const manager{make_master()};

  EXPECT_TRUE(manager.is_master());
  EXPECT_FALSE(manager.master_host().has_value());
  EXPECT_FALSE(manager.master_port().has_value());
}

TEST(ReplicationManagerTest, IsReplicaWhenMasterHostProvided)
{
  auto const manager{make_replica()};

  EXPECT_FALSE(manager.is_master());
  ASSERT_TRUE(manager.master_host().has_value());
  EXPECT_EQ(manager.master_host().value(), "localhost");
  ASSERT_TRUE(manager.master_port().has_value());
  EXPECT_EQ(manager.master_port().value(), 6379);
}

TEST(ReplicationManagerTest, ReplidIsFortyLowercaseHexCharacters)
{
  auto const manager{make_master()};

  EXPECT_TRUE(is_lower_hex_40(manager.replid()));
}

TEST(ReplicationManagerTest, ReplOffsetStartsAtZero)
{
  auto const manager{make_master()};

  EXPECT_EQ(manager.repl_offset(), 0);
}

TEST(ReplicationManagerTest, ReplidIsStableForAGivenInstance)
{
  auto const manager{make_master()};

  EXPECT_EQ(manager.replid(), manager.replid());
}

TEST(ReplicationManagerTest, DistinctInstancesGetDistinctReplids)
{
  auto const first{make_master()};
  auto const second{make_master()};

  EXPECT_NE(first.replid(), second.replid());
}
