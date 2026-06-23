#ifndef REDIS_CPP_MASTER_LINK_H
#define REDIS_CPP_MASTER_LINK_H

#include <cstddef>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>

#include "event-loop.h"

namespace redis_net
{

class MasterLink
{
public:
  MasterLink(EventLoop &event_loop,
             std::string master_host,
             int master_port,
             int listening_port);
  ~MasterLink() noexcept;

  MasterLink(MasterLink const &) = delete;
  MasterLink &operator=(MasterLink const &) = delete;

  void start();

private:
  enum class State
  {
    Connecting = 0,
    AwaitingPong,
    AwaitingReplconfPortReply,
    AwaitingReplconfCapaReply,
    AwaitingFullResync,
    Connected,
  };

  EventLoop &event_loop_;
  std::string master_host_;
  int master_port_{};
  int listening_port_{};
  int fd_{-1};
  State state_{State::Connecting};
  std::string read_buffer_{};
  std::string write_buffer_{};
  std::size_t write_offset_{};

  std::expected<void, std::string> try_start();
  void handle_writable();
  void handle_read();
  void handle_close();
  void handle_error(std::error_code error);

  bool finalize_connect();
  void send_command(std::string const &bytes);
  void process_handshake_replies();
  void advance_handshake(std::string_view line);
  void fail(std::string_view reason);
  void close_link() noexcept;
};

} // namespace redis_net

#endif // REDIS_CPP_MASTER_LINK_H
