#ifndef REDIS_CPP_REDIS_RADIX_TRIE_H
#define REDIS_CPP_REDIS_RADIX_TRIE_H
#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>


namespace redis_storage
{

template<typename Value>
class RadixTrie
{
public:
  bool insert(std::span<std::byte const> key, Value value);
  Value *find(std::span<std::byte const> key);
  template<typename Visitor>
  void for_each(std::span<std::byte const> start,
                std::span<std::byte const> end,
                Visitor visitor) const;
  template<typename Visitor>
  void read_from(std::span<std::byte const> start, Visitor visitor) const;
  std::optional<
          std::pair<std::vector<std::byte>, std::reference_wrapper<Value const>>>
  max() const;

  [[nodiscard]] size_t size() const { return size_; }
  [[nodiscard]] bool empty() const { return size_ == 0; }

private:
  struct Node
  {
    std::vector<std::byte> label;
    std::optional<Value> value;
    std::vector<Node> children;
  };
  Node root_{};
  size_t size_{};

  bool insert_(Node &node, std::span<std::byte const> remaining, Value value);
  std::size_t common_prefix_length(std::span<std::byte const> a,
                                   std::span<std::byte const> b);
  void insert_child_sort(std::vector<Node> &children, Node child);
  template<typename Visitor>
  void range_(Node const &node,
              std::vector<std::byte> &current_key,
              std::span<std::byte const> start,
              std::span<std::byte const> end,
              Visitor &visitor) const;
  template<typename Visitor>
  void read_from_(Node const &node,
                  std::vector<std::byte> &current_key,
                  std::span<std::byte const> start,
                  Visitor &visitor) const;
  void max_(Node const &node,
            std::vector<std::byte> &current_key,
            std::vector<std::byte> &max_key,
            Value const *&max_value) const;
};

template<typename Value>
bool RadixTrie<Value>::insert(std::span<std::byte const> const key, Value value)
{
  auto inserted{insert_(root_, key, std::move(value))};
  if (inserted) {
    ++size_;
  }
  return inserted;
}

template<typename Value>
Value *RadixTrie<Value>::find(std::span<std::byte const> key)
{
  auto current{&root_};
  while (!key.empty()) {
    auto const byte{key.front()};
    auto it{std::ranges::find_if(
            current->children,
            [byte](auto const &child)
            { return !child.label.empty() && child.label.front() == byte; })};

    if (it == current->children.end()) {
      return nullptr;
    }

    auto common_length{common_prefix_length(it->label, key)};
    if (common_length != it->label.size()) {
      return nullptr;
    }

    key = key.subspan(common_length);
    current = &*it;
  }
  return current->value.has_value() ? &current->value.value() : nullptr;
}

template<typename Value>
template<typename Visitor>
void RadixTrie<Value>::for_each(std::span<std::byte const> start,
                                std::span<std::byte const> end,
                                Visitor visitor) const
{
  if (std::ranges::lexicographical_compare(end, start)) {
    return;
  }
  std::vector<std::byte> current_key;
  range_(root_, current_key, start, end, visitor);
}

template<typename Value>
template<typename Visitor>
void RadixTrie<Value>::read_from(std::span<std::byte const> start,
                                 Visitor visitor) const
{
  if (empty()) {
    return;
  }
  std::vector<std::byte> current_key;
  read_from_(root_, current_key, start, visitor);
}

template<typename Value>
std::optional<
        std::pair<std::vector<std::byte>, std::reference_wrapper<Value const>>>
RadixTrie<Value>::max() const
{
  if (empty()) {
    return std::nullopt;
  }

  std::vector<std::byte> current_key;
  std::vector<std::byte> max_key;
  Value const *max_value{};
  max_(root_, current_key, max_key, max_value);
  return std::make_pair(std::move(max_key), std::cref(*max_value));
}

template<typename Value>
bool RadixTrie<Value>::insert_(Node &node,
                               std::span<std::byte const> remaining,
                               Value value)
{
  if (remaining.empty()) {
    if (node.value.has_value()) {
      return false;
    }
    node.value = std::move(value);
    return true;
  }

  auto const next{remaining.front()};
  // check if we have any child starting with this byte.
  auto it{std::ranges::find_if(
          node.children,
          [next](auto const &child)
          { return !child.label.empty() && child.label.front() == next; })};

  // we did not find any child starting with the current byte.
  // creating new leaf.
  if (it == node.children.end()) {
    Node new_child;
    new_child.label.assign(remaining.begin(), remaining.end());
    new_child.value = std::move(value);
    insert_child_sort(node.children, std::move(new_child));
    return true;
  }

  Node &child{*it};

  // we got a child, which has matching first byte.
  auto common_length{common_prefix_length(child.label, remaining)};
  // completely matched the child label.
  if (common_length == child.label.size()) {
    return insert_(child, remaining.subspan(common_length), std::move(value));
  }
  // completely matched remaining key size,
  //
  if (common_length == remaining.size()) {
    auto old_child = std::move(*it);
    old_child.label.erase(old_child.label.begin(),
                          old_child.label.begin() + common_length);
    Node new_node;
    auto prefix{remaining.subspan(0, common_length)};
    new_node.label.assign(prefix.begin(), prefix.end());
    new_node.value = std::move(value);
    insert_child_sort(new_node.children, std::move(old_child));

    *it = std::move(new_node);
    return true;
  }

  // if prefix did not completely match
  // neither child prefix nor remaining key sizes.
  auto old_child = std::move(*it);
  old_child.label.erase(old_child.label.begin(),
                        old_child.label.begin() + common_length);
  Node new_child;
  auto new_child_label{remaining.subspan(common_length)};
  new_child.label.assign(new_child_label.begin(), new_child_label.end());
  new_child.value = std::move(value);
  Node intermediate;
  auto intermediate_label{remaining.subspan(0, common_length)};
  intermediate.label.assign(intermediate_label.begin(),
                            intermediate_label.end());
  intermediate.children.reserve(2);
  insert_child_sort(intermediate.children, std::move(old_child));
  insert_child_sort(intermediate.children, std::move(new_child));
  *it = std::move(intermediate);
  return true;
}

template<typename Value>
std::size_t
RadixTrie<Value>::common_prefix_length(std::span<std::byte const> const a,
                                       std::span<std::byte const> const b)
{
  auto const [it_a, it_b]{std::ranges::mismatch(a, b)};
  return std::distance(a.begin(), it_a);
}

template<typename Value>
void RadixTrie<Value>::insert_child_sort(std::vector<Node> &children,
                                         Node child)
{
  auto it = std::ranges::lower_bound(children,
                                     child,
                                     [](auto const &a, auto const &b)
                                     { return a.label < b.label; });
  children.insert(it, std::move(child));
}

template<typename Value>
template<typename Visitor>
void RadixTrie<Value>::range_(Node const &node,
                              std::vector<std::byte> &current_key,
                              std::span<std::byte const> start,
                              std::span<std::byte const> end,
                              Visitor &visitor) const
{
  static auto less = [](std::span<std::byte const> const a,
                        std::span<std::byte const> const b)
  { return std::ranges::lexicographical_compare(a, b); };
  if (node.value.has_value() && !less(current_key, start) &&
      !less(end, current_key)) {
    visitor(current_key, *node.value);
  }

  for (auto const &child: node.children) {
    auto const current_key_size{current_key.size()};
    current_key.insert(
            current_key.end(), child.label.begin(), child.label.end());

    range_(child, current_key, start, end, visitor);

    current_key.resize(current_key_size);
  }
}

template<typename Value>
template<typename Visitor>
void RadixTrie<Value>::read_from_(Node const &node,
                                  std::vector<std::byte> &current_key,
                                  std::span<std::byte const> start,
                                  Visitor &visitor) const
{
  if (node.value.has_value() &&
      std::ranges::lexicographical_compare(start, current_key)) {
    visitor(current_key, *node.value);
  }

  for (auto const &child: node.children) {
    auto const current_key_size{current_key.size()};
    current_key.insert(
            current_key.end(), child.label.begin(), child.label.end());

    read_from_(child, current_key, start, visitor);

    current_key.resize(current_key_size);
  }
}

template<typename Value>
void RadixTrie<Value>::max_(Node const &node,
                            std::vector<std::byte> &current_key,
                            std::vector<std::byte> &max_key,
                            Value const *&max_value) const
{
  if (node.value.has_value()) {
    max_key = current_key;
    max_value = &*node.value;
  }
  if (!node.children.empty()) {
    auto const &child{node.children.back()};
    auto const current_key_size{current_key.size()};
    current_key.insert(
            current_key.end(), child.label.begin(), child.label.end());
    max_(child, current_key, max_key, max_value);
    current_key.resize(current_key_size);
  }
}

} // namespace redis_storage


#endif // REDIS_CPP_REDIS_RADIX_TRIE_H
