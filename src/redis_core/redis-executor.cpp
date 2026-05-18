#include "redis-executor.h"

#include <algorithm>
#include <charconv>
#include <format>
#include <functional>
#include <ranges>

#include "resp-encoder.h"

using namespace redis_core;
using namespace redis_storage;

namespace
{

template<class... Ts>
struct Overloaded : Ts...
{
  using Ts::operator()...;
};

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

RedisExecutor::RedisExecutor(RedisStorePtr p_redis_store) :
    p_store_(std::move(p_redis_store))
{
  handlers_.try_emplace("PING", 0, 0, &RedisExecutor::execute_ping);
  handlers_.try_emplace("ECHO", 0, 1, &RedisExecutor::execute_echo);
  handlers_.try_emplace("SET", 2, 4, &RedisExecutor::execute_set);
  handlers_.try_emplace("GET", 1, 1, &RedisExecutor::execute_get);
  handlers_.try_emplace(
          "RPUSH", 2, std::nullopt, &RedisExecutor::execute_rpush);
  handlers_.try_emplace(
          "LPUSH", 2, std::nullopt, &RedisExecutor::execute_lpush);
  handlers_.try_emplace("LLEN", 1, 1, &RedisExecutor::execute_llen);
  handlers_.try_emplace("LRANGE", 3, 3, &RedisExecutor::execute_lrange);
  handlers_.try_emplace("LPOP", 1, 2, &RedisExecutor::execute_lpop);
  handlers_.try_emplace(
          "BLPOP", 2, std::nullopt, &RedisExecutor::execute_blpop);
  handlers_.try_emplace("TYPE", 1, 1, &RedisExecutor::execute_type);
  handlers_.try_emplace("XADD", 4, std::nullopt, &RedisExecutor::execute_xadd);
  handlers_.try_emplace("XRANGE", 3, 3, &RedisExecutor::execute_xrange);
  handlers_.try_emplace(
          "XREAD", 3, std::nullopt, &RedisExecutor::execute_xread);
}

RedisExecutor::ExecutionResult RedisExecutor::execute(RedisCommand const &cmd,
                                                      CommandContext ctx)
{
  auto const &args{cmd.args()};
  if (auto const it{handlers_.find(cmd.name())}; it != handlers_.cend()) {
    auto &[min_argc, max_argc_op, handler] = it->second;
    if (args.size() < min_argc || args.size() > max_argc_op.value_or(INT_MAX)) {
      return ExecutionResult{
              ResultType::REPLY,
              encode_reply(SimpleError(std::format(
                      "Invalid number of arguments - {}", args.size())))};
    }
    auto [type, reply] = std::invoke(handler, this, args, ctx);
    return ExecutionResult{type, encode_reply(reply)};
  }
  return ExecutionResult{ResultType::REPLY,
                         encode_reply(SimpleError("Unknown command"))};
}

void RedisExecutor::remove_blocked_client(int const fd)
{
  if (blocked_clients_by_fd_.contains(fd)) {
    // Remove shared_ptr from client fd mapping
    // which means we keep weak_ptr-s in blocked_clients_by_key_ map,
    // those will be lazily pruned at some point during unblock_client_for_key
    // execution, when accessed for the key.
    // TODO: Maybe I need second thread, running periodically and doing
    // different type of clean ups, but that's for later.
    blocked_clients_by_fd_.erase(fd);
  }
}

void RedisExecutor::expire_blocked_clients(
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
RedisExecutor::get_next_blocked_client_timeout()
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

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_ping(std::span<std::string const> const, CommandContext)
{
  return ExecutionOutcome{ResultType::REPLY, SimpleString("PONG")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_set(std::span<std::string const> const args,
                           CommandContext)
{
  // TODO: Need to normalize accepted argument to uppercase,
  // so if users gives "px", we do not silently skip it.
  std::string_view constexpr px_arg{"PX"};
  RedisStore::SetOptions options;
  // TODO: Currently only supporting passive expiration.
  // I need to implement active one as well..................
  if (args.size() == 4 && args[2] == px_arg) {
    options.ttl_ms = std::chrono::milliseconds{std::stoi(args[3])};
  }
  p_store_->set(args[0], args[1], options);
  return ExecutionOutcome{ResultType::REPLY, SimpleString("OK")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_get(std::span<std::string const> const args,
                           CommandContext)
{
  if (auto const value{p_store_->get(args[0])}; value.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, BulkString(value.value())};
  }
  return ExecutionOutcome{ResultType::REPLY, NullBulkString{}};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_echo(std::span<std::string const> const args,
                            CommandContext)
{
  if (args.empty()) {
    return ExecutionOutcome{ResultType::REPLY, BulkString("")};
  }
  return ExecutionOutcome{ResultType::REPLY, BulkString(args[0])};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_rpush(std::span<std::string const> const args,
                             CommandContext)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const new_size{p_store_->rpush(args[0], args.subspan(1))};
      new_size.has_value()) {
    unblock_client_for_key(args[0]);
    return ExecutionOutcome{ResultType::REPLY, Integer{new_size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to push to the list")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_lpush(std::span<std::string const> const args,
                             CommandContext)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const new_size{p_store_->lpush(args[0], args.subspan(1))};
      new_size.has_value()) {
    unblock_client_for_key(args[0]);
    return ExecutionOutcome{ResultType::REPLY, Integer{new_size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to push to the list")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_llen(std::span<std::string const> const args,
                            CommandContext)
{
  // TODO: I need to implement some proper error handling,
  // not only here but throughout the project.
  if (auto const size{p_store_->llen(args[0])}; size.has_value()) {
    return ExecutionOutcome{ResultType::REPLY, Integer{size.value()}};
  }
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleError("Failed to get the list length")};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_lrange(std::span<std::string const> const args,
                              CommandContext)
{
  auto elements{
          p_store_->lrange(args[0], std::stoll(args[1]), std::stoll(args[2]))};
  auto values = elements |
                std::views::transform(
                        [](std::string &element)
                        { return RedisReply(BulkString(std::move(element))); });

  return ExecutionOutcome{
          ResultType::REPLY,
          Array(std::ranges::to<std::vector<RedisReply>>(values))};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_lpop(std::span<std::string const> const args,
                            CommandContext)
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

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_blpop(std::span<std::string const> args,
                             CommandContext ctx)
{
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

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_type(std::span<std::string const> const args,
                            CommandContext)
{
  return ExecutionOutcome{ResultType::REPLY,
                          SimpleString{p_store_->get_type(args[0])}};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_xadd(std::span<std::string const> const args,
                            CommandContext)
{
  std::vector<std::pair<std::string, std::string>> fields;
  if (args.size() % 2 != 0) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError("Invalid number of arguments")};
  }
  for (auto i = 2; i < args.size(); i += 2) {
    fields.emplace_back(args[i], args[i + 1]);
  }
  if (auto const id{p_store_->xadd(args[0], fields, args[1])}; id.has_value()) {
    unblock_client_for_key(args[0]);
    return ExecutionOutcome{ResultType::REPLY, BulkString(id.value())};
  } else {
    return ExecutionOutcome{ResultType::REPLY, SimpleError(id.error())};
  }
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_xrange(std::span<std::string const> const args,
                              CommandContext)
{
  auto const entries_res{p_store_->xrange(args[0], args[1], args[2])};
  if (!entries_res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError(entries_res.error())};
  }

  return ExecutionOutcome{ResultType::REPLY,
                          make_stream_entries_reply(entries_res.value())};
}

RedisExecutor::ExecutionOutcome
RedisExecutor::execute_xread(std::span<std::string const> const args,
                             CommandContext ctx)
{
  auto const options_res{parse_xread_options(args)};
  if (!options_res.has_value()) {
    return ExecutionOutcome{ResultType::REPLY,
                            SimpleError(options_res.error())};
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

  block_xread_client(std::move(ctx), std::move(request));

  return ExecutionOutcome{ResultType::BLOCKED, NullBulkString{}};
}

std::expected<RedisExecutor::XReadOptions, std::string>
RedisExecutor::parse_xread_options(
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

std::expected<RedisExecutor::XReadRequest, std::string>
RedisExecutor::make_xread_request(XReadOptions const &options)
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

std::expected<std::vector<RedisExecutor::RedisReply>, std::string>
RedisExecutor::read_xread_streams(
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

void RedisExecutor::block_xread_client(CommandContext ctx, XReadRequest request)
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

RedisExecutor::RedisReply
RedisExecutor::make_stream_entry_reply(StreamEntry const &entry) const
{
  std::vector<RedisReply> fields_reply;
  fields_reply.reserve(entry.fields.size() * 2);
  for (auto const &[field, value]: entry.fields) {
    fields_reply.emplace_back(BulkString(field));
    fields_reply.emplace_back(BulkString(value));
  }

  std::vector<RedisReply> entry_reply;
  entry_reply.reserve(2);
  entry_reply.emplace_back(BulkString(entry.id.to_string()));
  entry_reply.emplace_back(Array{std::move(fields_reply)});

  return Array{std::move(entry_reply)};
}

RedisExecutor::RedisReply RedisExecutor::make_stream_entries_reply(
        std::vector<StreamEntry> const &entries) const
{
  std::vector<RedisReply> entries_reply;
  entries_reply.reserve(entries.size());

  for (auto const &entry: entries) {
    entries_reply.emplace_back(make_stream_entry_reply(entry));
  }

  return Array{std::move(entries_reply)};
}

RedisExecutor::RedisReply RedisExecutor::make_xread_stream_reply(
        std::string const &key, std::vector<StreamEntry> const &entries) const
{
  std::vector<RedisReply> stream_reply;
  stream_reply.reserve(2);
  stream_reply.emplace_back(BulkString(key));
  stream_reply.emplace_back(make_stream_entries_reply(entries));

  return Array{std::move(stream_reply)};
}

std::string RedisExecutor::encode_reply(RedisReply const &reply)
{
  return std::visit(
          Overloaded{
                  [](std::monostate)
                  { return RespEncoder::encode_simple_error("Unknown state"); },
                  [](SimpleString const &val)
                  { return RespEncoder::encode_simple_string(val.value); },
                  [](BulkString const &val)
                  { return RespEncoder::encode_bulk_string(val.value); },
                  [](NullBulkString const &)
                  { return RespEncoder::encode_null_string(); },
                  [](SimpleError const &val)
                  { return RespEncoder::encode_simple_error(val.value); },
                  [](Integer const &val)
                  { return RespEncoder::encode_integer(val.value); },
                  [this](Array const &val)
                  {
                    std::vector<std::string> encoded_elements;
                    encoded_elements.reserve(val.values.size());
                    for (auto const &element: val.values) {
                      encoded_elements.emplace_back(encode_reply(element));
                    }
                    return RespEncoder::encode_array(encoded_elements);
                  },
                  [](NullArray const &)
                  { return RespEncoder::encode_null_array(); }},
          reply.value);
}

void RedisExecutor::unblock_client_for_key(std::string const &key)
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
