#include "redis-stream.h"

#include <charconv>
#include <chrono>
#include <cstring>
#include <limits>
#include <queue>
#include <ranges>
#include <bit>

using namespace redis_storage;

namespace
{

constexpr int64_t s_auto_generate_mark = -1;

std::expected<int64_t, std::string> parse_id_part(std::string_view const value)
{
  if (value.empty()) {
    return std::unexpected("invalid stream ID");
  }

  int64_t result{};
  auto const *const end = value.data() + value.size();
  if (auto const [ptr, ec] = std::from_chars(value.data(), end, result);
      ec != std::errc{} || ptr != end || result < 0) {
    return std::unexpected("invalid stream ID");
  }

  return result;
}

std::expected<StreamId, std::string>
parse_stream_id_or_milliseconds(std::string_view const value,
                                int64_t const default_sequence)
{
  return StreamId::parse(value).or_else(
          [value, default_sequence](std::string const &)
          {
            return parse_id_part(value).transform(
                    [default_sequence](int64_t const milliseconds)
                    { return StreamId{milliseconds, default_sequence}; });
          });
}

std::expected<StreamId, std::string>
parse_range_start_id(std::string_view const value)
{
  if (value == "-") {
    return StreamId{0, 0};
  }

  return parse_stream_id_or_milliseconds(value, 0);
}

std::expected<StreamId, std::string>
parse_range_end_id(std::string_view const value)
{
  if (value == "+") {
    return StreamId{std::numeric_limits<int64_t>::max(),
                    std::numeric_limits<int64_t>::max()};
  }

  return parse_stream_id_or_milliseconds(value,
                                         std::numeric_limits<int64_t>::max());
}

std::expected<StreamId, std::string>
parse_read_start_id(std::string_view const value)
{
  return parse_stream_id_or_milliseconds(value, 0);
}

} // namespace

std::expected<StreamId, std::string>
StreamId::parse(std::string_view const value)
{
  auto const delimiter_pos = value.find('-');
  if (delimiter_pos == std::string_view::npos ||
      value.find('-', delimiter_pos + 1) != std::string_view::npos) {
    if (value == "*") {
      return StreamId{s_auto_generate_mark, s_auto_generate_mark};
    }
    return std::unexpected("invalid stream ID");
  }

  auto const milliseconds_part = value.substr(0, delimiter_pos);
  auto const sequence_part = value.substr(delimiter_pos + 1);

  return parse_id_part(milliseconds_part)
          .and_then(
                  [sequence_part](int64_t const milliseconds)
                          -> std::expected<StreamId, std::string>
                  {
                    if (sequence_part == "*") {
                      return StreamId{milliseconds, s_auto_generate_mark};
                    }
                    return parse_id_part(sequence_part)
                            .transform(
                                    [milliseconds](int64_t const sequence)
                                    {
                                      return StreamId{milliseconds, sequence};
                                    });
                  });
}

std::string StreamId::to_string() const
{
  return std::to_string(milliseconds) + "-" + std::to_string(sequence);
}

StreamId::EncodedKey StreamId::encode() const
{
  EncodedKey key{};
  auto ms{static_cast<uint64_t>(milliseconds)};
  auto seq{static_cast<uint64_t>(sequence)};
  if constexpr (std::endian::native == std::endian::little) {
    ms = std::byteswap(ms);
    seq = std::byteswap(seq);
  }
  std::memcpy(key.data(), &ms, sizeof(ms));
  std::memcpy(key.data() + sizeof(ms), &seq, sizeof(seq));
  return key;
}

StreamId StreamId::decode(EncodedKey const &key)
{
  auto ms{*reinterpret_cast<uint64_t const *>(key.data())};
  auto seq{*reinterpret_cast<uint64_t const *>(key.data() + sizeof(ms))};
  if constexpr (std::endian::native == std::endian::little) {
    ms = std::byteswap(ms);
    seq = std::byteswap(seq);
  }
  return {static_cast<int64_t>(ms), static_cast<int64_t>(seq)};
}

std::expected<std::string, std::string> RedisStream::add(
        std::string_view const requested_id,
        std::span<std::pair<std::string, std::string> const> const fields)
{
  return StreamId::parse(requested_id)
          .and_then(
                  [this](StreamId stream_id)
                  {
                    auto_generate_stream_id(stream_id);
                    return validate_requested_id(stream_id);
                  })
          .and_then(
                  [this, fields](StreamId const stream_id)
                          -> std::expected<std::string, std::string>
                  {
                    return add_(stream_id, fields)
                            .transform([](StreamId const inserted_id)
                                       { return inserted_id.to_string(); });
                  });
}

std::expected<std::string, std::string>
RedisStream::add(std::span<std::pair<std::string, std::string> const> fields)
{
  return add_(get_next_id(), fields)
          .transform([](StreamId const inserted_id)
                     { return inserted_id.to_string(); });
}

std::expected<std::vector<StreamEntry>, std::string>
RedisStream::range(std::string_view const start,
                   std::string_view const end) const
{
  std::vector<StreamEntry> entries;
  if (entries_.empty()) {
    return std::vector<StreamEntry>{};
  }

  auto const start_id_res = parse_range_start_id(start);
  if (!start_id_res.has_value()) {
    return std::unexpected(start_id_res.error());
  }

  auto const end_id_res = parse_range_end_id(end);
  if (!end_id_res.has_value()) {
    return std::unexpected(end_id_res.error());
  }

  auto const start_id = start_id_res.value();
  auto const end_id = end_id_res.value();
  entries_.for_each(start_id.encode(),
                    end_id.encode(),
                    [&entries](auto const key, auto const &entry)
                    { entries.push_back(entry); });
  return entries;
}
std::expected<std::vector<StreamEntry>, std::string>
RedisStream::read(std::string_view const start) const
{
  std::vector<StreamEntry> entries;
  if (entries_.empty()) {
    return entries;
  }
  auto const start_id_res = parse_read_start_id(start);
  if (!start_id_res.has_value()) {
    return std::unexpected(start_id_res.error());
  }
  auto const start_id = start_id_res.value();
  entries_.read_from(start_id.encode(),
                     [&entries](auto const key, auto const &entry)
                     { entries.push_back(entry); });
  return entries;
}

std::optional<StreamId> RedisStream::last_id() const
{
  auto const max_res{entries_.max()};
  if (!max_res.has_value()) {
    return std::nullopt;
  }
  return max_res.value().second.get().id;
}

std::expected<StreamId, std::string> RedisStream::add_(
        StreamId const stream_id,
        std::span<std::pair<std::string, std::string> const> const fields)
{
  auto const inserted{entries_.insert(
          stream_id.encode(),
          StreamEntry{stream_id,
                      std::vector<std::pair<std::string, std::string>>{
                              fields.begin(), fields.end()}})};
  if (!inserted) {
    return std::unexpected("stream entry with given ID already exists");
  }

  return stream_id;
}

StreamId RedisStream::get_next_id() const
{
  auto const now_ms{std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count()};
  auto const max_res{entries_.max()};
  if (!max_res.has_value()) {
    return StreamId{now_ms, 0};
  }
  auto const [milliseconds, sequence]{max_res.value().second.get().id};
  if (now_ms > milliseconds) {
    return StreamId{now_ms, 0};
  }

  return StreamId{milliseconds, sequence + 1};
}

std::expected<StreamId, std::string>
RedisStream::validate_requested_id(StreamId const stream_id) const
{
  if (stream_id == StreamId{0, 0}) {
    return std::unexpected(
            "ERR The ID specified in XADD must be greater than 0-0");
  }
  if (auto const max_res{entries_.max()};
      max_res.has_value() && stream_id <= max_res.value().second.get().id) {
    return std::unexpected(
            "ERR The ID specified in XADD is equal or smaller than "
            "the target stream top item");
  }

  return stream_id;
}

void RedisStream::auto_generate_stream_id(StreamId &stream_id)
{
  if (stream_id.milliseconds == s_auto_generate_mark &&
      stream_id.sequence == s_auto_generate_mark) {
    stream_id = get_next_id();
  } else if (stream_id.sequence == s_auto_generate_mark) {
    std::priority_queue<StreamId> max_id;
    entries_.for_each(StreamId{stream_id.milliseconds, 0}.encode(),
                      StreamId{stream_id.milliseconds,
                               std::numeric_limits<int64_t>::max()}
                              .encode(),
                      [&max_id](auto const key, auto const &entry)
                      { max_id.push(entry.id); });
    if (!max_id.empty()) {
      stream_id.sequence = max_id.top().sequence + 1;
      return;
    }

    stream_id.sequence = stream_id.milliseconds == 0 ? 1 : 0;
  }
}
