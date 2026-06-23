#pragma once

#include <algorithm>
#include <ranges>
#include <string_view>

namespace redis_core::redis_command
{

constexpr char ascii_upper(char const c)
{
  if (c >= 'a' && c <= 'z') {
    return static_cast<char>(c - 'a' + 'A');
  }
  return c;
}

inline bool command_arg_equals(std::string_view const lhs,
                               std::string_view const rhs)
{
  return lhs.size() == rhs.size() &&
         std::ranges::equal(lhs, rhs, {}, ascii_upper, ascii_upper);
}

} // namespace redis_core::redis_command
