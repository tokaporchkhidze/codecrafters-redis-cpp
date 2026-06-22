#include "blocking-manager.h"

#include <algorithm>
#include <charconv>
#include <ranges>

#include "../reply-builder.h"

namespace redis_core::redis_command
{

namespace
{

std::expected<double, std::string> to_double(std::string_view const buffer)
{
  double result{};
  auto [ptr, ec]{std::from_chars(
          buffer.data(), buffer.data() + buffer.size(), result)};
  if (ec == std::errc{} && ptr == buffer.data() + buffer.size()) {
    return result;
  }
  return std::unexpected("invalid double value");
}

std::expected<int64_t, std::string> to_int64(std::string_view const buffer)
{
  int64_t result{};
  auto [ptr, ec]{std::from_chars(
          buffer.data(), buffer.data() + buffer.size(), result)};
  if (ec == std::errc{} && ptr == buffer.data() + buffer.size()) {
    return result;
  }
  return std::unexpected("invalid integer value");
}

constexpr char ascii_upper(char const c)
{
  if (c >= 'a' && c <= 'z') {
    return static_cast<char>(c - 'a' + 'A');
  }
  return c;
}

bool command_arg_equals(std::string_view const lhs, std::string_view const rhs)
{
  return lhs.size() == rhs.size() &&
         std::ranges::equal(lhs, rhs, {}, ascii_upper, ascii_upper);
}

std::chrono::steady_clock::time_point get_timeout(double const seconds)
{
  using clock = std::chrono::steady_clock;

  auto const timeout_duration = std::chrono::ceil<clock::duration>(
          std::chrono::duration<double>{seconds});

  return clock::now() + timeout_duration;
}

} // namespace

BlockingManager::BlockingManager(redis_storage::RedisStorePtr store,
                                 TransactionQuery is_in_transaction) :
    p_store_(std::move(store)),
    is_in_transaction_(std::move(is_in_transaction))
{
}

void BlockingManager::expire_blocked_clients(
        std::chrono::steady_clock::time_point const now)
{
  while (!blocked_clients_timeout_.empty()) {
    auto const &timeout{blocked_clients_timeout_.top()};
    auto const p_blocked_client = timeout.p_blocked_client.lock();
    if (!p_blocked_client ||
        p_blocked_client->timeout_tp.value() < timeout.deadline) {
      blocked_clients_timeout_.pop();
      continue;
    }

    if (timeout.deadline >= now) {
      break;
    }

    blocked_clients_timeout_.pop();
    p_blocked_client->callback(encode_reply(NullArray{}));
    blocked_clients_by_fd_.erase(p_blocked_client->client_fd);
  }
}

std::optional<std::chrono::steady_clock::time_point>
BlockingManager::get_next_blocked_client_timeout()
{
  while (!blocked_clients_timeout_.empty()) {
    auto const &blocked_client_timeout{blocked_clients_timeout_.top()};
    auto const p_blocked_client{blocked_client_timeout.p_blocked_client.lock()};
    if (!p_blocked_client) {
      blocked_clients_timeout_.pop();
      continue;
    }
    return p_blocked_client->timeout_tp;
  }
  return std::nullopt;
}

void BlockingManager::on_close_clean_up(int const fd)
{
  if (blocked_clients_by_fd_.contains(fd)) {
    // Remove shared_ptr from client fd mapping
    // which means we keep weak_ptr-s in blocked_clients_by_key_ map,
    // those will be lazily pruned at some point during unblock_client_for_key
    // execution, when accessed for the key.
    blocked_clients_by_fd_.erase(fd);
  }
}

ExecutionOutcome
BlockingManager::execute_lpop(std::span<std::string const> const args,
                              CommandContext const &)
{
  bool const has_count_arg = args.size() == 2;
  int64_t const count = has_count_arg ? std::stoll(args[1]) : 1;
  if (count < 0) {
    return ExecutionOutcome{
            ResultType::REPLY,
            SimpleError("value is out of range, must be positive")};
  }
  auto popped = p_store_->lpop(args[0], count);
  if (!popped.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, SimpleError("Wrong type")};
  }

  auto elements = std::move(popped.value());
  if (elements.empty()) {
    return ExecutionOutcome{ResultType::REPLY, NullBulkString{}};
  }

  if (has_count_arg) {
    auto values =
            elements |
            std::views::transform(
                    [](std::string &element)
                    { return RedisReply(BulkString(std::move(element))); });
    return ExecutionOutcome{
            ResultType::REPLY,
            Array(std::ranges::to<std::vector<RedisReply>>(values))};
  }

  return ExecutionOutcome{ResultType::REPLY,
                          BulkString(std::move(elements.front()))};
}

ExecutionOutcome
BlockingManager::execute_blpop(std::span<std::string const> const args,
                               CommandContext const &ctx)
{
  // if we are in MULTI mode,
  // just use lpop instead of blpop.
  if (is_in_transaction_(ctx.client_fd)) {
    return execute_lpop(args, ctx);
  }
  auto const conversion_res{to_double(args.back())};
  if (!conversion_res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError("invalid timeout value")};
  }
  std::optional<std::chrono::steady_clock::time_point> timeout_tp;
  if (conversion_res.value() < 0.0) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError("invalid timeout value")};
  }
  if (conversion_res.value() > 0.0) {
    timeout_tp = get_timeout(conversion_res.value());
  }
  for (auto const &key: args.subspan(0, args.size() - 1)) {
    auto popped = p_store_->lpop(key, 1);
    if (!popped.has_value()) {
      continue;
    }
    auto elements = std::move(popped.value());
    if (elements.empty()) {
      continue;
    }
    return ExecutionOutcome{
            ResultType::REPLY,
            Array({BulkString(key), BulkString(elements.front())})};
  }
  // If we get here, means no pop happened, need to block.
  auto blocked_client{std::make_shared<BlockedClient>(
          ctx.client_fd,
          ctx.callback,
          timeout_tp,
          [this](std::string const &key,
                 [[maybe_unused]] BlockedClient const &) -> UnblockOpResult
          {
            auto popped = p_store_->lpop(key, 1);
            if (!popped.has_value() || popped.value().empty()) {
              return {UnblockOpStatus::NOT_READY_STOP};
            }

            return {UnblockOpStatus::READY,
                    Array({BulkString(key),
                           BulkString(std::move(popped.value().front()))})};
          })};
  blocked_clients_by_fd_.emplace(ctx.client_fd, blocked_client);
  for (auto const &key: args.subspan(0, args.size() - 1)) {
    auto [it, _]{blocked_clients_by_key_.try_emplace(key)};
    it->second.emplace_back(blocked_client);
  }
  if (blocked_client->timeout_tp.has_value()) {
    blocked_clients_timeout_.emplace(blocked_client->timeout_tp.value(),
                                     blocked_client);
  }
  return ExecutionOutcome{ResultType::BLOCKED, NullBulkString{}};
}

ExecutionOutcome
BlockingManager::execute_xread(std::span<std::string const> const args,
                               CommandContext const &ctx)
{
  auto options_res{parse_xread_options(args)};
  if (!options_res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError(options_res.error())};
  }
  // if we are in MULTI mode, all blocking commands
  // become non-blocking.
  if (is_in_transaction_(ctx.client_fd)) {
    options_res->blocking = false;
  }

  auto request_res{make_xread_request(options_res.value())};
  if (!request_res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError(request_res.error())};
  }
  auto request = std::move(request_res.value());

  auto streams_reply_res{
          read_xread_streams(request.stream_keys, request.start_ids)};
  if (!streams_reply_res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError(streams_reply_res.error())};
  }

  if (auto &streams_reply = streams_reply_res.value();
      !streams_reply.empty() || !request.blocking) {
    return ExecutionOutcome{ResultType::REPLY, Array{std::move(streams_reply)}};
  }

  block_xread_client(ctx, std::move(request));

  return ExecutionOutcome{ResultType::BLOCKED, NullBulkString{}};
}

std::expected<BlockingManager::XReadOptions, std::string>
BlockingManager::parse_xread_options(
        std::span<std::string const> const args) const
{
  XReadOptions options;
  if (command_arg_equals(args[0], "BLOCK")) {
    if (args.size() < 4 || !command_arg_equals(args[2], "STREAMS")) {
      return std::unexpected("Invalid number of arguments");
    }

    auto const timeout_ms_res{to_int64(args[1])};
    if (!timeout_ms_res.has_value() || timeout_ms_res.value() < 0) {
      return std::unexpected("Invalid timeout value");
    }

    options.blocking = true;
    if (timeout_ms_res.value() > 0) {
      options.timeout_tp = std::chrono::steady_clock::now() +
                           std::chrono::milliseconds{timeout_ms_res.value()};
    }
    options.stream_args = args.subspan(3);
    return options;
  }

  if (!command_arg_equals(args[0], "STREAMS")) {
    return std::unexpected("Invalid number of arguments");
  }

  options.stream_args = args.subspan(1);
  return options;
}

std::expected<BlockingManager::XReadRequest, std::string>
BlockingManager::make_xread_request(XReadOptions const &options)
{
  if (options.stream_args.size() % 2 != 0) {
    return std::unexpected("Invalid number of arguments");
  }

  auto const stream_count{options.stream_args.size() / 2};
  std::vector<std::string> stream_keys;
  std::vector<std::string> start_ids;
  stream_keys.reserve(stream_count);
  start_ids.reserve(stream_count);

  for (std::size_t stream_idx{0}, start_idx = stream_count;
       stream_idx < stream_count;
       stream_idx += 1, start_idx += 1) {
    stream_keys.emplace_back(options.stream_args[stream_idx]);
    std::string start_id{options.stream_args[start_idx]};
    if (start_id == "$") {
      auto last_id = p_store_->xlastid(options.stream_args[stream_idx]);
      if (!last_id.has_value()) {
        return std::unexpected(last_id.error());
      }

      start_ids.emplace_back(last_id->has_value() ? last_id->value().to_string()
                                                  : "0-0");
    } else {
      start_ids.emplace_back(std::move(start_id));
    }
  }

  return XReadRequest{
          options.blocking,
          options.timeout_tp,
          std::move(stream_keys),
          std::move(start_ids),
  };
}

std::expected<std::vector<RedisReply>, std::string>
BlockingManager::read_xread_streams(
        std::vector<std::string> const &stream_keys,
        std::vector<std::string> const &start_ids) const
{
  std::vector<RedisReply> streams_reply;
  streams_reply.reserve(stream_keys.size());

  for (std::size_t stream_idx{0}; stream_idx < stream_keys.size();
       stream_idx += 1) {
    auto read_res{
            p_store_->xread(stream_keys[stream_idx], start_ids[stream_idx])};
    if (!read_res.has_value()) {
      return std::unexpected(read_res.error());
    }
    if (!read_res.value().empty()) {
      streams_reply.emplace_back(make_xread_stream_reply(
              stream_keys[stream_idx], read_res.value()));
    }
  }

  return streams_reply;
}

void BlockingManager::block_xread_client(CommandContext const &ctx,
                                         XReadRequest request)
{
  auto stream_keys = std::move(request.stream_keys);
  auto start_ids = std::move(request.start_ids);
  auto blocked_client{std::make_shared<BlockedClient>(
          ctx.client_fd,
          ctx.callback,
          request.timeout_tp,
          [this, stream_keys, start_ids](
                  [[maybe_unused]] std::string const &,
                  [[maybe_unused]] BlockedClient const &) -> UnblockOpResult
          {
            auto ready_streams_res{read_xread_streams(stream_keys, start_ids)};
            if (!ready_streams_res.has_value()) {
              return {UnblockOpStatus::READY,
                      SimpleError(ready_streams_res.error())};
            }

            auto ready_streams = std::move(ready_streams_res.value());
            if (ready_streams.empty()) {
              return {UnblockOpStatus::NOT_READY_STOP};
            }

            return {UnblockOpStatus::READY, Array{std::move(ready_streams)}};
          })};
  blocked_clients_by_fd_.emplace(ctx.client_fd, blocked_client);
  for (auto const &key: stream_keys) {
    auto [it, inserted]{blocked_clients_by_key_.try_emplace(key)};
    it->second.emplace_back(blocked_client);
  }
  if (blocked_client->timeout_tp.has_value()) {
    blocked_clients_timeout_.emplace(blocked_client->timeout_tp.value(),
                                     blocked_client);
  }
}

void BlockingManager::unblock_client_for_key(std::string const &key)
{
  if (auto const it{blocked_clients_by_key_.find(key)};
      it != blocked_clients_by_key_.cend()) {
    auto &blocked_clients = it->second;
    auto const now = std::chrono::steady_clock::now();
    while (!blocked_clients.empty()) {
      auto const p_blocked_client{blocked_clients.front().lock()};
      if (!p_blocked_client) {
        blocked_clients.pop_front();
        continue;
      }

      if (p_blocked_client->timeout_tp.has_value() &&
          p_blocked_client->timeout_tp.value() < now) {
        blocked_clients.pop_front();
        int const client_fd{p_blocked_client->client_fd};
        p_blocked_client->callback(encode_reply(NullArray{}));
        blocked_clients_by_fd_.erase(client_fd);
        continue;
      }

      auto [status,
            reply]{p_blocked_client->unblock_op(key, *p_blocked_client.get())};
      if (status == UnblockOpStatus::NOT_READY_STOP) {
        break;
      }
      if (status == UnblockOpStatus::NOT_READY_CONTINUE) {
        continue;
      }

      blocked_clients.pop_front();
      int const client_fd{p_blocked_client->client_fd};
      p_blocked_client->callback(encode_reply(reply));
      blocked_clients_by_fd_.erase(client_fd);
    }

    if (blocked_clients.empty()) {
      blocked_clients_by_key_.erase(it);
    }
  }
}

ExecutionOutcome
BlockingManager::run_blpop(std::span<std::string const> const args,
                           CommandContext const &ctx)
{
  return execute_blpop(args, ctx);
}

ExecutionOutcome
BlockingManager::run_xread(std::span<std::string const> const args,
                           CommandContext const &ctx)
{
  return execute_xread(args, ctx);
}

} // namespace redis_core::redis_command
