#ifndef REDIS_STARTER_CPP_ERRNO_ERROR_H
#define REDIS_STARTER_CPP_ERRNO_ERROR_H
#include <expected>
#include <string>
#include <system_error>
#include <print>

namespace redis_net
{

inline std::unexpected<std::string> unexpected_errno()
{
  int const err{errno};
  return std::unexpected(std::system_category().message(err));
}

inline void print_errno()
{
  int const err{errno};
  std::println(stderr, "{}", std::system_category().message(err));
}

}

#endif // REDIS_STARTER_CPP_ERRNO_ERROR_H
