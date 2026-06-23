#ifndef REDIS_CPP_REPLICATION_MANAGER_H
#define REDIS_CPP_REPLICATION_MANAGER_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "replication-service.h"

namespace redis_core::redis_command
{

// Owns replication state: the server role, its replication id and offset, and
// the address of the master when this server runs as a replica. Implements
// IReplicationService so commands can reach it through CommandDeps, and is
// shared with the networking layer (the master link) as replication grows.
class ReplicationManager : public IReplicationService
{
public:
  ReplicationManager(std::optional<std::string> master_host,
                     std::optional<int> master_port);

  [[nodiscard]] bool is_master() const override { return is_master_; }
  [[nodiscard]] std::string_view replid() const override { return replid_; }
  [[nodiscard]] std::int64_t repl_offset() const override
  {
    return master_repl_offset_;
  }

  [[nodiscard]] std::optional<std::string> const &master_host() const
  {
    return master_host_;
  }
  [[nodiscard]] std::optional<int> master_port() const { return master_port_; }

private:
  bool is_master_{};
  std::optional<std::string> master_host_;
  std::optional<int> master_port_;
  std::string replid_;
  std::int64_t master_repl_offset_{};
};

using ReplicationManagerPtr = std::shared_ptr<ReplicationManager>;

} // namespace redis_core::redis_command

#endif // REDIS_CPP_REPLICATION_MANAGER_H
