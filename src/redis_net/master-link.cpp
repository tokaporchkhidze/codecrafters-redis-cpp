#include "master-link.h"

#include <cerrno>
#include <format>
#include <netdb.h>
#include <print>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include "redis_core/resp-encoder.h"
#include "scope-guard.h"
#include "socket-utils.h"

using namespace redis_net;
using redis_core::RespEncoder;

namespace
{

std::size_t constexpr k_read_chunk_size{4096};
std::string_view constexpr k_terminator{"\r\n"};

std::string encode_command(std::vector<std::string_view> const &words)
{
  std::vector<std::string> bulk_strings;
  bulk_strings.reserve(words.size());
  for (auto const word: words) {
    bulk_strings.push_back(RespEncoder::encode_bulk_string(word));
  }
  return RespEncoder::encode_array(bulk_strings);
}

} // namespace

MasterLink::MasterLink(EventLoop &event_loop,
                       std::string master_host,
                       int const master_port,
                       int const listening_port) :
    event_loop_{event_loop},
    master_host_{std::move(master_host)},
    master_port_{master_port},
    listening_port_{listening_port}
{
}

MasterLink::~MasterLink() noexcept { close_link(); }

void MasterLink::start()
{
  if (auto const result{try_start()}; !result) {
    std::println(stderr, "master link: {}", result.error());
    close_link();
  }
}

std::expected<void, std::string> MasterLink::try_start()
{
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  addrinfo *result{nullptr};
  auto const service{std::to_string(master_port_)};
  if (auto const rc{getaddrinfo(
              master_host_.c_str(), service.c_str(), &hints, &result)};
      rc != 0) {
    return std::unexpected(std::format("failed to resolve {}:{} - {}",
                                       master_host_,
                                       master_port_,
                                       gai_strerror(rc)));
  }
  ScopeGuard const guard{result, [](addrinfo *p) { freeaddrinfo(p); }};

  fd_ = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
  if (fd_ == -1) {
    return std::unexpected(std::format(
            "socket() failed: {}", std::system_category().message(errno)));
  }

  try {
    set_nonblocking(fd_);
  } catch (std::system_error const &error) {
    return std::unexpected(std::string{error.what()});
  }

  if (connect(fd_, result->ai_addr, result->ai_addrlen) == -1 &&
      errno != EINPROGRESS) {
    return std::unexpected(std::format(
            "connect() failed: {}", std::system_category().message(errno)));
  }

  event_loop_.add(fd_,
                  EPOLLOUT | EPOLLRDHUP,
                  EventLoop::Handler{
                          .on_read = [this] { handle_read(); },
                          .on_write = [this] { handle_writable(); },
                          .on_close = [this] { handle_close(); },
                          .on_error = [this](std::error_code const error)
                          { handle_error(error); },
                  });
  return {};
}

void MasterLink::handle_writable()
{
  if (state_ == State::Connecting) {
    if (!finalize_connect()) {
      return;
    }
    state_ = State::AwaitingPong;
    send_command(encode_command({"PING"}));
  }

  while (write_offset_ < write_buffer_.size()) {
    auto const bytes_written{send(fd_,
                                  write_buffer_.data() + write_offset_,
                                  write_buffer_.size() - write_offset_,
                                  0)};
    if (bytes_written > 0) {
      write_offset_ += static_cast<std::size_t>(bytes_written);
      continue;
    }
    if (bytes_written == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return;
    }
    handle_error({errno, std::system_category()});
    return;
  }

  write_buffer_.clear();
  write_offset_ = 0;
  event_loop_.modify(fd_, EPOLLIN | EPOLLRDHUP);
}

void MasterLink::handle_read()
{
  while (true) {
    char chunk[k_read_chunk_size];
    auto const bytes_read{recv(fd_, chunk, sizeof(chunk), 0)};
    if (bytes_read > 0) {
      read_buffer_.append(chunk, static_cast<std::size_t>(bytes_read));
      continue;
    }
    if (bytes_read == 0) {
      handle_close();
      return;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    handle_error({errno, std::system_category()});
    return;
  }
  process_handshake_replies();
}

void MasterLink::handle_close()
{
  std::println(stderr, "master link: connection to master closed");
  close_link();
}

void MasterLink::handle_error(std::error_code const error)
{
  std::println(stderr, "master link: error: {}", error.message());
  close_link();
}

bool MasterLink::finalize_connect()
{
  int socket_error{};
  socklen_t length{sizeof(socket_error)};
  if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &socket_error, &length) == -1) {
    handle_error({errno, std::system_category()});
    return false;
  }
  if (socket_error != 0) {
    handle_error({socket_error, std::system_category()});
    return false;
  }
  return true;
}

void MasterLink::send_command(std::string const &bytes)
{
  write_buffer_ += bytes;
  event_loop_.modify(fd_, EPOLLIN | EPOLLOUT | EPOLLRDHUP);
}

void MasterLink::process_handshake_replies()
{
  while (state_ != State::Connected && fd_ != -1) {
    auto const pos{read_buffer_.find(k_terminator)};
    if (pos == std::string::npos) {
      break; // reply line not fully received yet
    }
    std::string const line{read_buffer_, 0, pos};
    read_buffer_.erase(0, pos + k_terminator.size());
    advance_handshake(line);
  }
}

void MasterLink::advance_handshake(std::string_view const line)
{
  if (line.starts_with('-')) {
    fail(line);
    return;
  }

  switch (state_) {
    case State::AwaitingPong:
      send_command(encode_command(
              {"REPLCONF", "listening-port", std::to_string(listening_port_)}));
      state_ = State::AwaitingReplconfPortReply;
      break;
    case State::AwaitingReplconfPortReply:
      send_command(encode_command({"REPLCONF", "capa", "psync2"}));
      state_ = State::AwaitingReplconfCapaReply;
      break;
    case State::AwaitingReplconfCapaReply:
      send_command(encode_command({"PSYNC", "?", "-1"}));
      state_ = State::AwaitingFullResync;
      break;
    case State::AwaitingFullResync:
      state_ = State::Connected;
      std::println("master link: handshake complete ({})", line);
      break;
    default:
      break;
  }
}

void MasterLink::fail(std::string_view const reason)
{
  std::println(stderr, "master link: handshake failed: {}", reason);
  close_link();
}

void MasterLink::close_link() noexcept
{
  if (fd_ == -1) {
    return;
  }
  int const fd_to_close{fd_};
  fd_ = -1;
  try {
    event_loop_.remove(fd_to_close);
  } catch (...) {
  }
  close(fd_to_close);
}
