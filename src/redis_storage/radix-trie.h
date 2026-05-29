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

// TODO: Desperate need for memory optimization.
//    range_ read_from_ search needs speed optimization.
namespace redis_storage
{

template<typename T>
concept RadixValue =
        std::copy_constructible<T> && std::assignable_from<T &, T const &>;

template<typename F, typename T>
concept RadixVisitor = std::invocable<F, std::span<std::byte const>, T const &>;

template<RadixValue Value>
class RadixTrie
{
public:
  bool insert(std::span<std::byte const> key, Value value);
  Value *find(std::span<std::byte const> key);
  template<RadixVisitor<Value> Visitor>
  void for_each(std::span<std::byte const> start,
                std::span<std::byte const> end,
                Visitor visitor) const;
  template<RadixVisitor<Value> Visitor>
  void read_from(std::span<std::byte const> start, Visitor visitor) const;
  std::optional<std::pair<std::vector<std::byte>,
                          std::reference_wrapper<Value const>>>
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

  struct CompareResult
  {
    int result; // -1 <; 0 ==; 1 >
    bool candidate_prefix_of_bound;
    bool bound_prefix_of_candidate;
  };

  struct ByteLess
  {
    constexpr bool operator()(std::byte const a,
                              std::byte const b) const noexcept
    {
      return std::to_integer<unsigned>(a) < std::to_integer<unsigned>(b);
    }
  };

  template<class R1, class R2>
  static bool less_span(R1 const &a, R2 const &b) noexcept
  {
    return std::ranges::lexicographical_compare(a, b, ByteLess{});
  }

  static CompareResult
  compare_labels(std::span<std::byte const> const bound,
                    std::span<std::byte const> const current_key,
                    std::span<std::byte const> const child_label) noexcept
  {
    auto const bound_size{bound.size()};
    auto const base_sz{current_key.size()};
    auto const candidate_size{base_sz + child_label.size()};

    std::size_t i{};
    for (; i < candidate_size && i < bound_size; ++i) {
      auto const candidate_byte{i < base_sz ? current_key[i]
                                            : child_label[i - base_sz]};
      auto const bound_byte{bound[i]};
      if (ByteLess{}(candidate_byte, bound_byte)) {
        return {-1, false, false};
      }
      if (ByteLess{}(bound_byte, candidate_byte)) {
        return {+1, false, false};
      }
    }

    if (i == candidate_size && i == bound_size) {
      return {0, true, true};
    }
    if (i == candidate_size) {
      return {-1, true, false};
    }
    return {+1, false, true};
  }

  Node root_{};
  size_t size_{};
  size_t longest_key_length_{};

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

template<RadixValue Value>
bool RadixTrie<Value>::insert(std::span<std::byte const> const key, Value value)
{
  auto inserted{insert_(root_, key, std::move(value))};
  if (inserted) {
    ++size_;
    longest_key_length_ = std::max(longest_key_length_, key.size());
  }
  return inserted;
}

template<RadixValue Value>
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

template<RadixValue Value>
template<RadixVisitor<Value> Visitor>
void RadixTrie<Value>::for_each(std::span<std::byte const> start,
                                std::span<std::byte const> end,
                                Visitor visitor) const
{
  if (less_span(end, start)) {
    return;
  }
  std::vector<std::byte> current_key;
  current_key.reserve(longest_key_length_);
  range_(root_, current_key, start, end, visitor);
}

template<RadixValue Value>
template<RadixVisitor<Value> Visitor>
void RadixTrie<Value>::read_from(std::span<std::byte const> start,
                                 Visitor visitor) const
{
  if (empty()) {
    return;
  }
  std::vector<std::byte> current_key;
  current_key.reserve(longest_key_length_);
  read_from_(root_, current_key, start, visitor);
}

template<RadixValue Value>
std::optional<
        std::pair<std::vector<std::byte>, std::reference_wrapper<Value const>>>
RadixTrie<Value>::max() const
{
  if (empty()) {
    return std::nullopt;
  }

  std::vector<std::byte> current_key;
  current_key.reserve(longest_key_length_);
  std::vector<std::byte> max_key;
  max_key.reserve(longest_key_length_);
  Value const *max_value{};
  max_(root_, current_key, max_key, max_value);
  return std::make_pair(std::move(max_key), std::cref(*max_value));
}

template<RadixValue Value>
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

template<RadixValue Value>
std::size_t
RadixTrie<Value>::common_prefix_length(std::span<std::byte const> const a,
                                       std::span<std::byte const> const b)
{
  auto const [it_a, it_b]{std::ranges::mismatch(a, b)};
  return std::distance(a.begin(), it_a);
}

template<RadixValue Value>
void RadixTrie<Value>::insert_child_sort(std::vector<Node> &children,
                                         Node child)
{
  auto it = std::ranges::lower_bound(
          children,
          child,
          [](auto const &a, auto const &b)
          {
            return less_span(std::span<std::byte const>(a.label),
                             std::span<std::byte const>(b.label));
          });
  children.insert(it, std::move(child));
}

template<RadixValue Value>
template<typename Visitor>
void RadixTrie<Value>::range_(Node const &node,
                              std::vector<std::byte> &current_key,
                              std::span<std::byte const> start,
                              std::span<std::byte const> end,
                              Visitor &visitor) const
{
  if (node.value.has_value() && !less_span(std::span(current_key), start) &&
      !less_span(end, std::span(current_key))) {
    visitor(std::span<std::byte const>(current_key.data(), current_key.size()),
            *node.value);
  }

  for (auto const &child: node.children) {

    // Here we can decide which subtrees to traverse and which to
    // completely ignore.
    // the candidate label is less and not a prefix of lower bound.
    if (CompareResult const compare_res{
                compare_labels(start,
                                  std::span(current_key),
                                  std::span<std::byte const>(child.label))};
        compare_res.result < 0 && !compare_res.candidate_prefix_of_bound) {
      continue;
    }

    bool break_after_next_child{};
    if (CompareResult const compare_res{
                compare_labels(end,
                                  std::span(current_key),
                                  std::span<std::byte const>(child.label))};
        compare_res.result == 0) {
      break_after_next_child = true;
    } else if (compare_res.result > 0 ||
               compare_res.bound_prefix_of_candidate) {
      break;
    }

    auto const current_key_size{current_key.size()};
    current_key.insert(
            current_key.end(), child.label.begin(), child.label.end());

    range_(child, current_key, start, end, visitor);

    current_key.resize(current_key_size);
    if (break_after_next_child) {
      break;
    }
  }
}

template<RadixValue Value>
template<typename Visitor>
void RadixTrie<Value>::read_from_(Node const &node,
                                  std::vector<std::byte> &current_key,
                                  std::span<std::byte const> start,
                                  Visitor &visitor) const
{
  if (node.value.has_value() && less_span(start, std::span(current_key))) {
    visitor(std::span<std::byte const>(current_key), *node.value);
  }

  for (auto const &child: node.children) {
    if (CompareResult const compare_res{
                compare_labels(start,
                                  std::span(current_key),
                                  std::span<std::byte const>(child.label))};
        compare_res.result < 0 && !compare_res.candidate_prefix_of_bound) {
      continue;
    }

    auto const current_key_size{current_key.size()};
    current_key.insert(
            current_key.end(), child.label.begin(), child.label.end());

    read_from_(child, current_key, start, visitor);

    current_key.resize(current_key_size);
  }
}

template<RadixValue Value>
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
