#include <stdexcept>
#include <type_traits>

#include <gtest/gtest.h>

#include "redis_net/scope-guard.h"

using redis_net::ScopeGuard;

namespace
{

struct Resource
{
  int id{};
};

} // namespace

TEST(ScopeGuardCleanupTest, RunsCleanupOnceWithCapturedResourceOnScopeExit)
{
  int call_count{0};
  int seen_id{-1};
  {
    Resource const resource{.id = 42};
    ScopeGuard const guard{resource,
                           [&](Resource const r)
                           {
                             ++call_count;
                             seen_id = r.id;
                           }};
  }
  EXPECT_EQ(call_count, 1);
  EXPECT_EQ(seen_id, 42);
}

TEST(ScopeGuardCleanupTest, RunsCleanupWhenScopeExitsViaException)
{
  int call_count{0};
  try {
    ScopeGuard const guard{7, [&](int const) { ++call_count; }};
    throw std::runtime_error{"boom"};
  } catch (std::runtime_error const &) {
  }
  EXPECT_EQ(call_count, 1);
}

TEST(ScopeGuardCleanupTest, IsNeitherCopyableNorMovable)
{
  using Guard = ScopeGuard<int, void (*)(int)>;
  static_assert(!std::is_copy_constructible_v<Guard>);
  static_assert(!std::is_copy_assignable_v<Guard>);
  static_assert(!std::is_move_constructible_v<Guard>);
  static_assert(!std::is_move_assignable_v<Guard>);
  SUCCEED();
}
