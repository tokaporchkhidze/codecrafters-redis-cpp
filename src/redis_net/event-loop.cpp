#include "event-loop.h"

#include <cerrno>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <system_error>
#include <utility>
#include <vector>

using namespace redis_net;

EventLoop::EventLoop()
{
  epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
  if (epoll_fd_ == -1) {
    throw std::system_error{errno, std::system_category()};
  }
}

EventLoop::~EventLoop() noexcept
{
  if (epoll_fd_ != -1) {
    close(epoll_fd_);
  }
}

void EventLoop::add(int const fd, std::uint32_t const events, Handler handler)
{
  epoll_event new_event{};
  new_event.events = events;
  new_event.data.fd = fd;
  if (auto const res{epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &new_event)};
      res == -1) {
    throw std::system_error{errno, std::system_category()};
  }
  handlers_.insert_or_assign(fd, std::move(handler));
}

void EventLoop::modify(int const fd, std::uint32_t const events) const
{
  if (!handlers_.contains(fd)) {
    return;
  }
  epoll_event new_event{};
  new_event.events = events;
  new_event.data.fd = fd;
  if (auto const res{epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &new_event)};
      res == -1) {
    throw std::system_error{errno, std::system_category()};
  }
}

void EventLoop::remove(int const fd)
{
  if (!handlers_.contains(fd)) {
    return;
  }
  if (auto const res{epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr)};
      res == -1 && errno != EBADF && errno != ENOENT) {
    throw std::system_error{errno, std::system_category()};
  }
  handlers_.erase(fd);
}

void EventLoop::run()
{
  running_ = true;
  while (running_) {
    std::vector<epoll_event> events(100);
    int const ready_fds{epoll_wait(
            epoll_fd_, events.data(), static_cast<int>(events.size()), 1000)};
    if (ready_fds == -1) {
      if (errno == EINTR) {
        continue;
      }
      throw std::system_error{errno, std::system_category()};
    }

    for (auto i{0}; i < ready_fds; i++) {
      auto const current_fd{events[i].data.fd};
      auto const handler_it{handlers_.find(current_fd)};
      if (handler_it == handlers_.end()) {
        continue;
      }

      auto [on_read, on_write, on_close, on_error]{handler_it->second};
      auto const current_events{events[i].events};
      if (current_events & EPOLLERR) {
        int socket_error{};
        socklen_t socket_error_len{sizeof(socket_error)};
        if (getsockopt(current_fd,
                       SOL_SOCKET,
                       SO_ERROR,
                       &socket_error,
                       &socket_error_len) == -1) {
          socket_error = errno;
        }
        if (on_error) {
          on_error({socket_error, std::system_category()});
        }
        continue;
      }
      if (current_events & (EPOLLHUP | EPOLLRDHUP)) {
        if (on_close) {
          on_close();
        }
        continue;
      }
      if (current_events & EPOLLIN) {
        if (on_read) {
          on_read();
        }
        if (!handlers_.contains(current_fd)) {
          continue;
        }
      }
      if (current_events & EPOLLOUT) {
        if (on_write) {
          on_write();
        }
      }
    }
  }
}

void EventLoop::stop() { running_ = false; }
