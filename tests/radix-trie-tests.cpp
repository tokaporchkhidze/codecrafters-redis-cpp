#include <algorithm>
#include <cstddef>
#include <gtest/gtest.h>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#define private public
#include "redis_storage/radix-trie.h"
#undef private

namespace redis_storage::tests
{
namespace
{

  std::span<std::byte const> bytes(std::string_view value)
  {
    return std::as_bytes(std::span{value.data(), value.size()});
  }

  std::vector<std::byte> byte_vector(std::string_view value)
  {
    auto const value_bytes{bytes(value)};
    return {value_bytes.begin(), value_bytes.end()};
  }

  RadixTrie<int>::Node child_with_label(std::string_view label)
  {
    RadixTrie<int>::Node child;
    child.label = byte_vector(label);
    return child;
  }

  std::vector<std::string> child_labels(RadixTrie<int>::Node const &node)
  {
    std::vector<std::string> labels;
    labels.reserve(node.children.size());
    for (auto const &child: node.children) {
      std::string label;
      label.reserve(child.label.size());
      std::ranges::transform(child.label,
                             std::back_inserter(label),
                             [](std::byte value)
                             { return static_cast<char>(value); });
      labels.push_back(std::move(label));
    }
    return labels;
  }

  std::vector<int> range_values(RadixTrie<int> const &trie,
                                std::string_view start,
                                std::string_view end)
  {
    std::vector<int> values;
    trie.for_each(bytes(start),
                  bytes(end),
                  [&values](std::span<std::byte> key, int const value)
                  { values.push_back(value); });
    return values;
  }

  std::string string_from_bytes(std::vector<std::byte> const &value)
  {
    std::string result;
    result.reserve(value.size());
    std::ranges::transform(value,
                           std::back_inserter(result),
                           [](std::byte byte)
                           { return static_cast<char>(byte); });
    return result;
  }

  TEST(RadixTrieInsertTest, InsertsNewKeyAndReportsDuplicateOnSameKey)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("key"), 1));
    EXPECT_FALSE(trie.insert(bytes("key"), 2));
    EXPECT_FALSE(trie.insert(bytes("key"), 3));
  }

  TEST(RadixTrieInsertTest, InsertsAndUpdatesEmptyKey)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes(""), 1));
    EXPECT_FALSE(trie.insert(bytes(""), 2));
  }

  TEST(RadixTrieInsertTest, InsertsPrefixOfExistingKey)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("foobar"), 1));
    EXPECT_TRUE(trie.insert(bytes("foo"), 2));

    EXPECT_FALSE(trie.insert(bytes("foo"), 3));
    EXPECT_FALSE(trie.insert(bytes("foobar"), 4));
  }

  TEST(RadixTrieInsertTest, InsertsExtensionOfExistingKey)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("foo"), 1));
    EXPECT_TRUE(trie.insert(bytes("foobar"), 2));

    EXPECT_FALSE(trie.insert(bytes("foo"), 3));
    EXPECT_FALSE(trie.insert(bytes("foobar"), 4));
  }

  TEST(RadixTrieInsertTest, InsertsKeysThatDivergeInsideCompressedLabel)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("foobar"), 1));
    EXPECT_TRUE(trie.insert(bytes("foobaz"), 2));

    EXPECT_FALSE(trie.insert(bytes("foobar"), 3));
    EXPECT_FALSE(trie.insert(bytes("foobaz"), 4));
  }

  TEST(RadixTrieInsertTest, InsertsIndependentBranches)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("alpha"), 1));
    EXPECT_TRUE(trie.insert(bytes("bravo"), 2));

    EXPECT_FALSE(trie.insert(bytes("alpha"), 3));
    EXPECT_FALSE(trie.insert(bytes("bravo"), 4));
  }

  TEST(RadixTrieInsertTest, InsertChildSortKeepsChildrenOrderedByLabel)
  {
    RadixTrie<int> trie;
    RadixTrie<int>::Node node;

    trie.insert_child_sort(node.children, child_with_label("delta"));
    trie.insert_child_sort(node.children, child_with_label("alpha"));
    trie.insert_child_sort(node.children, child_with_label("charlie"));
    trie.insert_child_sort(node.children, child_with_label("bravo"));

    EXPECT_EQ(child_labels(node),
              (std::vector<std::string>{"alpha", "bravo", "charlie", "delta"}));
  }

  TEST(RadixTrieInsertTest, InsertUsesSortedChildInsertionForRootBranches)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("delta"), 1));
    EXPECT_TRUE(trie.insert(bytes("alpha"), 2));
    EXPECT_TRUE(trie.insert(bytes("charlie"), 3));
    EXPECT_TRUE(trie.insert(bytes("bravo"), 4));

    EXPECT_EQ(child_labels(trie.root_),
              (std::vector<std::string>{"alpha", "bravo", "charlie", "delta"}));
  }

  TEST(RadixTrieInsertTest, InsertUsesSortedChildInsertionWhenSplittingLabel)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("food"), 1));
    EXPECT_TRUE(trie.insert(bytes("foam"), 2));

    ASSERT_EQ(trie.root_.children.size(), 1);
    RadixTrie<int>::Node const &shared_prefix{trie.root_.children.front()};

    EXPECT_EQ(shared_prefix.label, byte_vector("fo"));
    EXPECT_EQ(child_labels(shared_prefix),
              (std::vector<std::string>{"am", "od"}));
  }

  TEST(RadixTrieRangeTest, VisitsValuesInLexicographicKeyOrderAcrossBranches)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("delta"), 4));
    EXPECT_TRUE(trie.insert(bytes("alpha"), 1));
    EXPECT_TRUE(trie.insert(bytes("charlie"), 3));
    EXPECT_TRUE(trie.insert(bytes("bravo"), 2));

    EXPECT_EQ(range_values(trie, "", "zzzz"), (std::vector<int>{1, 2, 3, 4}));
  }

  TEST(RadixTrieRangeTest, UsesInclusiveStartAndEndBounds)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("alpha"), 1));
    EXPECT_TRUE(trie.insert(bytes("bravo"), 2));
    EXPECT_TRUE(trie.insert(bytes("charlie"), 3));

    EXPECT_EQ(range_values(trie, "bravo", "bravo"), (std::vector<int>{2}));
    EXPECT_EQ(range_values(trie, "alpha", "bravo"), (std::vector<int>{1, 2}));
    EXPECT_EQ(range_values(trie, "bravo", "charlie"), (std::vector<int>{2, 3}));
  }

  TEST(RadixTrieRangeTest, HandlesCompressedLabelsAndPrefixKeys)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("foobar"), 1));
    EXPECT_TRUE(trie.insert(bytes("foobaz"), 2));
    EXPECT_TRUE(trie.insert(bytes("foo"), 3));
    EXPECT_TRUE(trie.insert(bytes("fop"), 4));
    EXPECT_TRUE(trie.insert(bytes("bar"), 5));

    EXPECT_EQ(range_values(trie, "foo", "fooz"), (std::vector<int>{3, 1, 2}));
  }

  TEST(RadixTrieRangeTest, ReturnsEmptyWhenBoundsAreReversed)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("alpha"), 1));
    EXPECT_TRUE(trie.insert(bytes("bravo"), 2));

    EXPECT_TRUE(range_values(trie, "bravo", "alpha").empty());
  }

  TEST(RadixTrieRangeTest, ReturnsEmptyWhenNoKeyMatchesBounds)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("alpha"), 1));
    EXPECT_TRUE(trie.insert(bytes("charlie"), 3));

    EXPECT_TRUE(range_values(trie, "bravo", "bravoz").empty());
  }

  TEST(RadixTrieRangeTest, IncludesEmptyKeyWhenItIsInRange)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes(""), 0));
    EXPECT_TRUE(trie.insert(bytes("alpha"), 1));

    EXPECT_EQ(range_values(trie, "", ""), (std::vector<int>{0}));
    EXPECT_EQ(range_values(trie, "", "alpha"), (std::vector<int>{0, 1}));
  }

  TEST(RadixTrieMaxTest, ReturnsEmptyForEmptyTrie)
  {
    RadixTrie<int> trie;

    EXPECT_FALSE(trie.max().has_value());
  }

  TEST(RadixTrieMaxTest, ReturnsLargestKeyAndValueAcrossBranches)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("delta"), 4));
    EXPECT_TRUE(trie.insert(bytes("alpha"), 1));
    EXPECT_TRUE(trie.insert(bytes("charlie"), 3));
    EXPECT_TRUE(trie.insert(bytes("bravo"), 2));

    auto const max{trie.max()};

    ASSERT_TRUE(max.has_value());
    EXPECT_EQ(string_from_bytes(max->first), "delta");
    EXPECT_EQ(max->second.get(), 4);
  }

  TEST(RadixTrieMaxTest, ReturnsLargestCompressedPrefixKey)
  {
    RadixTrie<int> trie;

    EXPECT_TRUE(trie.insert(bytes("foo"), 1));
    EXPECT_TRUE(trie.insert(bytes("foobar"), 2));
    EXPECT_TRUE(trie.insert(bytes("fooz"), 3));
    EXPECT_TRUE(trie.insert(bytes("fop"), 4));

    auto const max{trie.max()};

    ASSERT_TRUE(max.has_value());
    EXPECT_EQ(string_from_bytes(max->first), "fop");
    EXPECT_EQ(max->second.get(), 4);
  }

} // namespace
} // namespace redis_storage::tests
