#ifndef REDIS_CPP_REDIS_STREAM_H
#define REDIS_CPP_REDIS_STREAM_H

#include <expected>
#include <map>
#include <span>
#include <string>
#include <vector>

namespace redis_storage
{

struct StreamId
{
  int64_t milliseconds{};
  int64_t sequence{};

  auto operator<=>(StreamId const &) const = default;

  static std::expected<StreamId, std::string> parse(std::string_view value);
  [[nodiscard]] std::string to_string() const;
};

struct StreamEntry
{
  StreamId id;
  std::vector<std::pair<std::string, std::string>> fields;
};

class RedisStream
{
public:
  std::expected<std::string, std::string>
  add(std::string_view requested_id,
      std::span<std::pair<std::string, std::string> const> fields);

  std::expected<std::string, std::string>
  add(std::span<std::pair<std::string, std::string> const> fields);

  [[nodiscard]] std::expected<std::vector<StreamEntry>, std::string>
  range(std::string_view start, std::string_view end) const;

  [[nodiscard]] std::expected<std::vector<StreamEntry>, std::string>
  read(std::string_view start) const;

private:
  std::map<StreamId, StreamEntry> entries_;

  std::expected<StreamId, std::string>
  add_(StreamId stream_id,
       std::span<std::pair<std::string, std::string> const> fields);

  [[nodiscard]] StreamId get_next_id() const;

  [[nodiscard]] std::expected<StreamId, std::string>
  validate_requested_id(StreamId stream_id) const;

  void auto_generate_stream_id(StreamId &stream_id);
};

} // namespace redis_storage

#endif // REDIS_CPP_REDIS_STREAM_H
