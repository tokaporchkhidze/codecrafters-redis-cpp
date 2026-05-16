#include "redis-store.h"

#include <ranges>

using namespace redis_storage;

namespace
{

std::string store_error_message(RedisStore::StoreError const error)
{
  switch (error) {
  case RedisStore::StoreError::WRONG_TYPE:
    return "Wrong type";
  case RedisStore::StoreError::KEY_NOT_FOUND:
    return "Key not found";
  }
  return "Store error";
}

} // namespace

void RedisStore::set(std::string const &key,
                     std::string const &value,
                     SetOptions const &options)
{
  std::optional<std::chrono::steady_clock::time_point> expires_at;
  if (options.ttl_ms.has_value()) {
    expires_at = std::chrono::steady_clock::now() + options.ttl_ms.value();
  }
  map_.insert_or_assign(key,
                        RedisValue{
                                value,
                                expires_at,
                        });
}

std::optional<std::string> RedisStore::get(std::string const &key)
{
  // TODO: For now just handling key-value pairs.
  if (auto const it{map_.find(key)}; it != map_.cend()) {
    auto &[value, expires_at] = it->second;
    if (expires_at.has_value() &&
        expires_at.value() < std::chrono::steady_clock::now()) {
      map_.erase(it);
      return std::nullopt;
    }
    // TODO: This is incorrect, assumes value type always string.
    return std::get<std::string>(value);
  }
  return std::nullopt;
}

std::expected<int64_t, RedisStore::StoreError>
RedisStore::rpush(std::string const &key,
                  std::span<std::string const> const values)
{
  return push_to_list<PushSide::RIGHT>(key, values);
}

std::expected<int64_t, RedisStore::StoreError>
RedisStore::lpush(std::string const &key,
                  std::span<std::string const> const values)
{
  return push_to_list<PushSide::LEFT>(key, values);
}

std::expected<int64_t, RedisStore::StoreError>
RedisStore::llen(std::string const &key)
{
  if (auto const it{map_.find(key)}; it != map_.cend()) {
    if (!std::holds_alternative<List>(it->second.value)) {
      return std::unexpected(StoreError::WRONG_TYPE);
    }
    auto const &list = std::get<List>(it->second.value);
    return list.size();
  }
  return 0;
}

std::vector<std::string>
RedisStore::lrange(std::string const &key, int64_t start, int64_t stop)
{
  auto const it{map_.find(key)};
  if (it == map_.cend() || !std::holds_alternative<List>(it->second.value)) {
    return {};
  }
  auto const &list = std::get<List>(it->second.value);
  // normalize indexes.
  if (start < 0) {
    start = static_cast<int64_t>(list.size()) + start;
  }
  if (stop < 0) {
    stop = static_cast<int64_t>(list.size()) + stop;
  }
  // if start is still negative, make it 0.
  if (start < 0) {
    start = 0;
  }
  if (start >= list.size() || start > stop) {
    return {};
  }
  auto const first{list.begin() + start};
  auto const last{list.begin() +
                  std::min(static_cast<size_t>(stop) + 1, list.size())};

  std::vector<std::string> result;
  result.reserve(last - first);
  std::ranges::copy(first, last, std::back_inserter(result));
  return result;
}

std::expected<std::vector<std::string>, RedisStore::StoreError>
RedisStore::lpop(std::string const &key, int64_t const count)
{
  auto const it{map_.find(key)};
  if (it == map_.cend()) {
    return {};
  }
  if (!std::holds_alternative<List>(it->second.value)) {
    return std::unexpected(StoreError::WRONG_TYPE);
  }
  auto &list = std::get<List>(it->second.value);
  auto elements_to_pop = std::min(static_cast<size_t>(count), list.size());
  std::vector<std::string> removed_elements;
  removed_elements.reserve(elements_to_pop);
  while (elements_to_pop > 0) {
    removed_elements.push_back(list.front());
    list.pop_front();
    elements_to_pop--;
  }
  return removed_elements;
}

std::string RedisStore::get_type(std::string const &key)
{
  if (auto const it{map_.find(key)}; it != map_.cend()) {
    if (std::holds_alternative<List>(it->second.value)) {
      return "list";
    }
    if (std::holds_alternative<std::string>(it->second.value)) {
      return "string";
    }
    if (std::holds_alternative<RedisStream>(it->second.value)) {
      return "stream";
    }
  }
  return "none";
}
std::expected<std::string, std::string> RedisStore::xadd(
        std::string const &key,
        std::span<std::pair<std::string, std::string> const> const fields,
        std::string const &requested_id)
{
  auto const stream = get_or_create_stream(key);
  if (!stream.has_value()) {
    return std::unexpected(store_error_message(stream.error()));
  }

  if (requested_id == "*") {
    return stream->get().add(fields);
  }
  return stream->get().add(requested_id, fields);
}

std::expected<std::vector<StreamEntry>, std::string>
RedisStore::xrange(std::string const &key,
                   std::string const &start,
                   std::string const &end)
{
  auto const stream = find_stream(key);
  if (!stream.has_value() && stream.error() == StoreError::KEY_NOT_FOUND) {
    return std::vector<StreamEntry>{};
  }
  if (!stream.has_value()) {
    return std::unexpected(store_error_message(stream.error()));
  }

  return stream->get().range(start, end);
}

std::expected<std::vector<StreamEntry>, std::string>
RedisStore::xread(std::string const &key, std::string const &start)
{
  auto const stream = find_stream(key);
  if (!stream.has_value() && stream.error() == StoreError::KEY_NOT_FOUND) {
    return std::vector<StreamEntry>{};
  }
  if (!stream.has_value()) {
    return std::unexpected(store_error_message(stream.error()));
  }

  return stream->get().read(start);
}


std::expected<std::reference_wrapper<RedisStore::List>, RedisStore::StoreError>
RedisStore::get_or_create_list(std::string const &key)
{
  auto [it, inserted] = map_.try_emplace(key,
                                         RedisValue{
                                                 List{},
                                                 std::nullopt,
                                         });

  if (it->second.expires_at.has_value() &&
      it->second.expires_at.value() < std::chrono::steady_clock::now()) {
    map_.erase(it);
    it = map_.try_emplace(key,
                          RedisValue{
                                  List{},
                                  std::nullopt,
                          })
                 .first;
  }

  if (!std::holds_alternative<List>(it->second.value)) {
    return std::unexpected(StoreError::WRONG_TYPE);
  }

  return std::ref(std::get<List>(it->second.value));
}

std::expected<std::reference_wrapper<RedisStream>, RedisStore::StoreError>
RedisStore::get_or_create_stream(std::string const &key)
{
  auto [it, inserted] = map_.try_emplace(key,
                                         RedisValue{
                                                 RedisStream{},
                                                 std::nullopt,
                                         });
  if (inserted) {
    return std::ref(std::get<RedisStream>(it->second.value));
  }
  if (!std::holds_alternative<RedisStream>(it->second.value)) {
    return std::unexpected(StoreError::WRONG_TYPE);
  }
  return std::ref(std::get<RedisStream>(it->second.value));
}

std::expected<std::reference_wrapper<RedisStream const>, RedisStore::StoreError>
RedisStore::find_stream(std::string const &key) const
{
  auto const it{map_.find(key)};
  if (it == map_.cend()) {
    return std::unexpected(StoreError::KEY_NOT_FOUND);
  }
  if (!std::holds_alternative<RedisStream>(it->second.value)) {
    return std::unexpected(StoreError::WRONG_TYPE);
  }
  return std::cref(std::get<RedisStream>(it->second.value));
}
