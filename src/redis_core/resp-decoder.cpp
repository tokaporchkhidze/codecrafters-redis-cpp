#include "resp-decoder.h"

#include <charconv>

using namespace redis_core;

namespace
{

std::expected<int32_t, std::string> to_int(std::string_view const buffer)
{
  int32_t result{};
  auto [ptr, ec]{std::from_chars(
          buffer.data(), buffer.data() + buffer.size(), result, 10)};
  if (ec == std::errc{} && ptr == buffer.data() + buffer.size()) {
    if (result < 0) {
      return std::unexpected("negative integer value, not accepted as length");
    }
    return result;
  }
  return std::unexpected("invalid integer value");
}

} // namespace

void RespDecoder::reset() noexcept
{
  state_ = State::PARSE_TYPE;
  bytes_consumed_ = 0;
  array_size_ = 0;
  bulk_string_length_ = 0;
  args_.clear();
}

RespDecoder::ParseResult RespDecoder::try_parse(std::string_view buffer)
{
  ParseResult result{
          .status = Status::Incomplete,
          .bytes_consumed = 0,
          .args = {},
  };
  while (!buffer.empty() && result.status == Status::Incomplete) {
    auto const parseRes{parse(buffer)};
    if (!parseRes.has_value()) {
      result.error_message = parseRes.error();
      result.status = Status::Error;
      return result;
    }
    buffer.remove_prefix(bytes_consumed_);
    result.bytes_consumed += bytes_consumed_;
    bytes_consumed_ = 0;
    result.status = parseRes.value();
  }
  if (result.status == Status::Complete) {
    result.args = std::move(args_);
  }
  return result;
}

std::expected<RespDecoder::Status, std::string>
RespDecoder::parse(std::string_view const buffer)
{
  switch (state_) {
    case State::PARSE_TYPE:
    {
      char const type_prefix{buffer[0]};
      if (auto const type_it{s_prefixToState.find(type_prefix)};
          type_it != s_prefixToState.cend()) {
        state_ = type_it->second;
        bytes_consumed_++;
      } else {
        return std::unexpected("invalid type prefix");
      }
      return Status::Incomplete;
    }
    case State::PARSE_ARRAY_LENGTH:
    {
      auto const lengthRes{parse_length(buffer)};
      if (!lengthRes.has_value()) {
        return std::unexpected(lengthRes.error());
      }
      if (lengthRes.value() == -1) {
        return Status::Incomplete;
      }
      args_.reserve(lengthRes.value());
      array_size_ = lengthRes.value();
      if (array_size_ == 0) {
        return Status::Complete;
      }
      // Got array size, need to start parsing elements.
      state_ = State::PARSE_TYPE;
      return Status::Incomplete;
    }
    case State::PARSE_BULK_STRING_LENGTH:
    {
      auto const lengthRes{parse_length(buffer)};
      if (!lengthRes.has_value()) {
        return std::unexpected(lengthRes.error());
      }
      if (lengthRes.value() == -1) {
        return Status::Incomplete;
      }
      bulk_string_length_ = lengthRes.value();
      state_ = State::PARSE_BULK_STRING_DATA;
      return Status::Incomplete;
    }
    case State::PARSE_BULK_STRING_DATA:
    {
      if (buffer.size() >= bulk_string_length_ + s_terminator.size()) {
        // we got whole data, now check validity
        if (buffer.substr(bulk_string_length_, s_terminator.size()) !=
            s_terminator) {
          return std::unexpected("invalid bulk string terminator");
        }
        args_.emplace_back(buffer.substr(0, bulk_string_length_));
        bytes_consumed_ += bulk_string_length_ + s_terminator.size();
        if (args_.size() == array_size_) {
          return Status::Complete;
        }
        state_ = State::PARSE_TYPE;
      }
      return Status::Incomplete;
    }
    default:
    {
      return std::unexpected("invalid state");
    }
  }
}

std::expected<int32_t, std::string>
RespDecoder::parse_length(std::string_view buffer)
{
  if (auto const idx{buffer.find(s_terminator)};
      idx != std::string_view::npos) {
    bytes_consumed_ += idx + s_terminator.size();
    return to_int(buffer.substr(0, idx));
  }
  return -1;
}
