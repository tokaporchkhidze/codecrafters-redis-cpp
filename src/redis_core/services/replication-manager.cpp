#include "replication-manager.h"

#include <random>
#include <utility>

namespace redis_core::redis_command
{

namespace
{

std::string generate_replid()
{
  static std::string_view constexpr hex_digits{"0123456789abcdef"};
  std::random_device rd{};
  std::mt19937 gen{rd()};
  std::uniform_int_distribution dist{0, 15};

  std::string id;
  id.reserve(40);
  for (int i{0}; i < 40; ++i) {
    id.push_back(hex_digits[dist(gen)]);
  }
  return id;
}

} // namespace

ReplicationManager::ReplicationManager(std::optional<std::string> master_host,
                                       std::optional<int> const master_port) :
    is_master_(!master_host.has_value()),
    master_host_(std::move(master_host)),
    master_port_(master_port),
    replid_(generate_replid())
{
}

} // namespace redis_core::redis_command
