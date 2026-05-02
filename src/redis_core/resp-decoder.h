#ifndef REDIS_CPP_RESP_PARSER_H
#define REDIS_CPP_RESP_PARSER_H

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace redis_core
{

class RespDecoder
{
public:
  enum class Status
  {
    Incomplete = 0,
    Complete,
    Error,
  };

  struct ParseResult
  {
    Status status{Status::Incomplete};
    std::size_t bytes_consumed{};
    std::vector<std::string> args{};
    std::string error_message{};
  };

  void reset() noexcept;

  ParseResult try_parse(std::string_view buffer);

private:
  enum class State
  {
    PARSE_TYPE = 0,
    PARSE_ARRAY_LENGTH,
    PARSE_BULK_STRING_LENGTH,
    PARSE_BULK_STRING_DATA,
  };

  using PrefixToState = std::unordered_map<char, State>;
  State state_{State::PARSE_TYPE};
  size_t bytes_consumed_{};
  size_t array_size_{};
  size_t bulk_string_length_{};
  std::vector<std::string> args_{};


  static std::string_view constexpr s_terminator{"\r\n"};
  static char constexpr s_bulk_string_prefix{'$'};
  inline static PrefixToState s_prefixToState{
          {'*', State::PARSE_ARRAY_LENGTH},
          {'$', State::PARSE_BULK_STRING_LENGTH},
  };

  std::expected<Status, std::string> parse(std::string_view buffer);

  std::expected<int32_t, std::string> parse_length(std::string_view buffer);
};

} // namespace redis_core

#endif // REDIS_CPP_RESP_PARSER_H
