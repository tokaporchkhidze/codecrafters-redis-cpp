#include "redis-stream.h"

#include <charconv>
#include <chrono>

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

std::expected<StreamId, std::string> RedisStream::add_(
        StreamId const stream_id,
        std::span<std::pair<std::string, std::string> const> const fields)
{
  auto const inserted =
          entries_.try_emplace(
                          stream_id,
                          StreamEntry{
                                  stream_id,
                                  std::vector<
                                          std::pair<std::string, std::string>>{
                                          fields.begin(), fields.end()}})
                  .second;
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
  if (entries_.empty()) {
    return StreamId{now_ms, 0};
  }
  const auto &[milliseconds, sequence]{entries_.crbegin()->first};
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

  if (!entries_.empty() && stream_id <= entries_.crbegin()->first) {
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
    auto const upper = entries_.upper_bound(StreamId{
            stream_id.milliseconds, std::numeric_limits<int64_t>::max()});

    if (upper != entries_.begin()) {
      if (auto const previous = std::prev(upper);
          previous->first.milliseconds == stream_id.milliseconds) {
        stream_id.sequence = previous->first.sequence + 1;
        return;
      }
    }

    stream_id.sequence = stream_id.milliseconds == 0 ? 1 : 0;
  }
}
