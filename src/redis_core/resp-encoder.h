#ifndef REDIS_CPP_RESP_ENCODER_H
#define REDIS_CPP_RESP_ENCODER_H

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace redis_core
{

class RespEncoder
{
public:
  static std::string encode_bulk_string(std::string_view message);
  static std::string encode_simple_string(std::string_view message);
  static std::string encode_null_string();
  static std::string encode_simple_error(std::string_view message);
  static std::string encode_integer(std::int64_t value);
  static std::string encode_array(std::vector<std::string> const &values);

private:
  static std::string constexpr s_terminator{"\r\n"};
  static char constexpr s_bulk_string_prefix{'$'};
  static char constexpr s_simple_string_prefix{'+'};
  static char constexpr s_simple_error_prefix{'-'};

};

}

#endif // REDIS_CPP_RESP_ENCODER_H
