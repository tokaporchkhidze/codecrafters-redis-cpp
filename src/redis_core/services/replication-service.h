#ifndef REDIS_CPP_REPLICATION_SERVICE_H
#define REDIS_CPP_REPLICATION_SERVICE_H

#include <cstdint>
#include <string_view>

namespace redis_core::redis_command
{

class IReplicationService
{
public:
  virtual ~IReplicationService() = default;

  [[nodiscard]] virtual bool is_master() const = 0;
  [[nodiscard]] virtual std::string_view replid() const = 0;
  [[nodiscard]] virtual std::int64_t repl_offset() const = 0;
};

} // namespace redis_core::redis_command

#endif // REDIS_CPP_REPLICATION_SERVICE_H
