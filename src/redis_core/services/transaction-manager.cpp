#include "transaction-manager.h"

#include <algorithm>

namespace redis_core::redis_command
{

TransactionManager::TransactionManager(redis_storage::RedisStorePtr store,
                                       IBlockingService &blocking,
                                       bool const is_master) :
    p_store_(std::move(store)), blocking_(blocking), is_master_(is_master)
{
}

ExecutionOutcome
TransactionManager::run_multi(std::span<std::string const>,
                              CommandContext const &ctx)
{
  if (clients_transaction_queue_.contains(ctx.client_fd)) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError("ERR MULTI calls can not be nested")};
  }
  clients_transaction_queue_.try_emplace(ctx.client_fd);
  return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
}

ExecutionOutcome
TransactionManager::run_exec(std::span<std::string const>,
                             CommandContext const &ctx)
{
  if (!clients_transaction_queue_.contains(ctx.client_fd)) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError("ERR EXEC without MULTI")};
  }
  auto exec_clear = [this, &ctx]
  {
    clients_transaction_queue_.erase(ctx.client_fd);
    clear_watched_keys(ctx.client_fd);
  };
  if (dirty_clients_.contains(ctx.client_fd)) {
    exec_clear();
    return ExecutionOutcome{ResultType::REPLY, NullArray{}};
  }
  auto const it{clients_transaction_queue_.find(ctx.client_fd)};
  auto &transaction_queue{it->second};
  Array transaction_replies;
  transaction_replies.values.reserve(transaction_queue.size());
  CommandDeps deps{p_store_, blocking_, *this, is_master_};
  while (!transaction_queue.empty()) {
    auto &current_cmd{transaction_queue.front()};
    auto [type, reply] = current_cmd.command->execute(
            current_cmd.args, current_cmd.ctx, deps);
    transaction_replies.values.emplace_back(std::move(reply));
    transaction_queue.pop();
  }
  exec_clear();
  return ExecutionOutcome{ResultType::REPLY, std::move(transaction_replies)};
}

ExecutionOutcome
TransactionManager::run_discard(std::span<std::string const>,
                                CommandContext const &ctx)
{
  if (clients_transaction_queue_.contains(ctx.client_fd)) {
    clients_transaction_queue_.erase(ctx.client_fd);
    clear_watched_keys(ctx.client_fd);
    return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("ERR DISCARD without MULTI")};
}

ExecutionOutcome
TransactionManager::run_watch(std::span<std::string const> const args,
                              CommandContext const &ctx)
{
  if (clients_transaction_queue_.contains(ctx.client_fd)) {
    return ExecutionOutcome{
            ResultType::REPLY,
            SimpleError("ERR WATCH inside MULTI is not allowed")};
  }

  auto const client_it{
          watched_keys_by_clients_.try_emplace(ctx.client_fd).first};

  for (auto const &key: args) {

    client_it->second.emplace(key);
    auto const clients_by_key_it{
            watched_clients_by_key_.try_emplace(key).first};
    clients_by_key_it->second.emplace(ctx.client_fd);
  }
  return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
}

ExecutionOutcome
TransactionManager::run_unwatch(std::span<std::string const>,
                                CommandContext const &ctx)
{
  clear_watched_keys(ctx.client_fd);
  return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
}

bool TransactionManager::is_in_transaction(int const client_fd) const
{
  return clients_transaction_queue_.contains(client_fd);
}

void TransactionManager::queue_command(ICommand *const command,
                                       std::vector<std::string> args,
                                       CommandContext ctx)
{
  auto const it{clients_transaction_queue_.find(ctx.client_fd)};
  it->second.emplace(command, std::move(args), std::move(ctx));
}

void TransactionManager::clear_watched_keys(int const client_fd)
{
  auto const it{watched_keys_by_clients_.find(client_fd)};
  if (it == watched_keys_by_clients_.end()) {
    return;
  }
  std::ranges::for_each(
          it->second,
          [this, client_fd](std::string const &key)
          {
            if (auto const clients_it{watched_clients_by_key_.find(key)};
                clients_it != watched_clients_by_key_.cend()) {
              clients_it->second.erase(client_fd);
              if (clients_it->second.empty()) {
                watched_clients_by_key_.erase(clients_it);
              }
            }
          });
  dirty_clients_.erase(client_fd);
  watched_keys_by_clients_.erase(it);
}

void TransactionManager::mark_watched_key_dirty(std::string const &key)
{
  auto const it{watched_clients_by_key_.find(key)};
  if (it == watched_clients_by_key_.cend()) {
    return;
  }
  for (auto const client_fd: it->second) {
    dirty_clients_.emplace(client_fd);
  }
}

void TransactionManager::on_close_clean_up(int const fd)
{
  clients_transaction_queue_.erase(fd);
  clear_watched_keys(fd);
}

} // namespace redis_core::redis_command
