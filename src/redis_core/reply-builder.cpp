#include "reply-builder.h"

#include <variant>

#include "resp-encoder.h"

namespace redis_core::redis_command
{

namespace
{

template<class... Ts>
struct Overloaded : Ts...
{
  using Ts::operator()...;
};

} // namespace

RedisReply make_stream_entry_reply(redis_storage::StreamEntry const &entry)
{
  std::vector<RedisReply> fields_reply;
  fields_reply.reserve(entry.fields.size() * 2);
  for (auto const &[field, value]: entry.fields) {
    fields_reply.emplace_back(BulkString(field));
    fields_reply.emplace_back(BulkString(value));
  }

  std::vector<RedisReply> entry_reply;
  entry_reply.reserve(2);
  entry_reply.emplace_back(BulkString(entry.id.to_string()));
  entry_reply.emplace_back(Array{std::move(fields_reply)});

  return Array{std::move(entry_reply)};
}

RedisReply make_stream_entries_reply(
        std::vector<redis_storage::StreamEntry> const &entries)
{
  std::vector<RedisReply> entries_reply;
  entries_reply.reserve(entries.size());

  for (auto const &entry: entries) {
    entries_reply.emplace_back(make_stream_entry_reply(entry));
  }

  return Array{std::move(entries_reply)};
}

RedisReply make_xread_stream_reply(
        std::string const &key,
        std::vector<redis_storage::StreamEntry> const &entries)
{
  std::vector<RedisReply> stream_reply;
  stream_reply.reserve(2);
  stream_reply.emplace_back(BulkString(key));
  stream_reply.emplace_back(make_stream_entries_reply(entries));

  return Array{std::move(stream_reply)};
}

std::string encode_reply(RedisReply const &reply)
{
  return std::visit(
          Overloaded{
                  [](std::monostate)
                  { return RespEncoder::encode_simple_error("Unknown state"); },
                  [](SimpleString const &val)
                  { return RespEncoder::encode_simple_string(val.value); },
                  [](BulkString const &val)
                  { return RespEncoder::encode_bulk_string(val.value); },
                  [](NullBulkString const &)
                  { return RespEncoder::encode_null_string(); },
                  [](SimpleError const &val)
                  { return RespEncoder::encode_simple_error(val.value); },
                  [](Integer const &val)
                  { return RespEncoder::encode_integer(val.value); },
                  [](Array const &val)
                  {
                    std::vector<std::string> encoded_elements;
                    encoded_elements.reserve(val.values.size());
                    for (auto const &element: val.values) {
                      encoded_elements.emplace_back(encode_reply(element));
                    }
                    return RespEncoder::encode_array(encoded_elements);
                  },
                  [](NullArray const &)
                  { return RespEncoder::encode_null_array(); }},
          reply.value);
}

} // namespace redis_core::redis_command
