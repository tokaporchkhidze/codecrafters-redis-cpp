//
// Created by toka on 5/2/26.
//

#include "resp-encoder.h"

#include <format>

using namespace redis_core;

std::string RespEncoder::encode_bulk_string(std::string_view const message)
{
  return std::format("{}{}{}{}{}",
                     s_bulk_string_prefix,
                     message.size(),
                     s_terminator,
                     message,
                     s_terminator);
}

std::string RespEncoder::encode_simple_string(std::string_view const message)
{
  return std::format("{}{}{}", s_simple_string_prefix, message, s_terminator);
}

std::string RespEncoder::encode_null_string() { return "$-1\r\n"; }

std::string RespEncoder::encode_simple_error(std::string_view const message)
{
  return std::format("{}{}{}", s_simple_error_prefix, message, s_terminator);
}

std::string RespEncoder::encode_integer(std::int64_t value)
{
  return std::format(":{}{}", value, s_terminator);
}
