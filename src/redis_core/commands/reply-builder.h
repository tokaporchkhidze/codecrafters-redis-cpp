#ifndef REDIS_CPP_REPLY_BUILDER_H
#define REDIS_CPP_REPLY_BUILDER_H

#include <string>
#include <vector>

#include "command-interface.h"
#include "redis_storage/redis-store.h"

namespace redis_core::redis_command
{

RedisReply make_stream_entry_reply(redis_storage::StreamEntry const &entry);

RedisReply make_stream_entries_reply(
        std::vector<redis_storage::StreamEntry> const &entries);

RedisReply make_xread_stream_reply(
        std::string const &key,
        std::vector<redis_storage::StreamEntry> const &entries);

std::string encode_reply(RedisReply const &reply);

} // namespace redis_core::redis_command

#endif // REDIS_CPP_REPLY_BUILDER_H
