#ifndef REDIS_CPP_SOCKET_UTILS_H
#define REDIS_CPP_SOCKET_UTILS_H
#include <cerrno>
#include <fcntl.h>
#include <system_error>

namespace redis_net
{

inline void set_nonblocking(int const fd)
{
  int const flags{fcntl(fd, F_GETFL, 0)};
  if (flags == -1) {
    throw std::system_error{errno, std::system_category()};
  }
  if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    throw std::system_error{errno, std::system_category()};
  }
}

} // namespace redis_net

#endif // REDIS_CPP_SOCKET_UTILS_H
