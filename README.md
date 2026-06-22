# Redis Clone in C++

A small Redis-compatible server built in C++ as part of the CodeCrafters
"Build Your Own Redis" challenge.

The project implements a TCP server, RESP command parsing, command execution,
and an in-memory data store for strings, lists, streams, and a byte-oriented
radix trie used for lexicographic indexing and traversal.

## Goal

I started this project to get back into C++ after taking about a year-long break
from the language.

Redis was also something I was generally curious about. I wanted to understand
more of its internals by building a smaller version myself: how the protocol is
parsed, how commands are dispatched, how data structures are represented, and how
the server handles multiple client connections.

## Features

- RESP request parsing and response encoding
- Non-blocking TCP server using `epoll`
- In-memory Redis-style storage
- Byte-oriented radix trie for lexicographic key/index traversal (Lots of room for optimizations still.)
- Multiple client connections
- Passive key expiry with `SET key value PX milliseconds`
- Blocking operations with timeout support for lists and streams
- Basic transaction support with command queuing and watched-key invalidation

## Supported Commands

### Connection and utility commands

- `PING`
- `ECHO`
- `TYPE`

### String commands

- `SET`
  - Stores string values.
  - Supports millisecond expiry with `SET key value PX milliseconds`.
- `GET`
- `INCR`
  - Creates missing keys with value `1`.
  - Returns an error for non-integer strings, overflow, or wrong-type values.

### List commands

- `RPUSH`
- `LPUSH`
- `LLEN`
- `LRANGE`
- `LPOP`
  - Supports both `LPOP key` and `LPOP key count`.
- `BLPOP`
  - Supports blocking pop across one or more keys with a timeout.

### Stream commands

- `XADD`
  - Supports explicit IDs and `*` auto-generated IDs.
- `XRANGE`
- `XREAD`
  - Supports reading from one or more streams.
  - Supports `BLOCK milliseconds`.
  - Supports `$` as the starting ID for blocking reads.

### Transaction commands

- `MULTI`
  - Starts a per-client transaction and queues subsequent commands.
- `EXEC`
  - Executes queued commands and returns their replies as an array.
  - Returns a null array when a watched key was modified before execution.
- `DISCARD`
- `WATCH`
  - Watches one or more keys for optimistic transaction invalidation.
- `UNWATCH`
  - Clears all watched keys for the current client.
- `DISCARD`
  - Clears the queued transaction commands and watched keys.

Blocking commands are executed in non-blocking mode while queued inside a
transaction.

## Current Scope

This project is a learning-focused Redis clone, not a complete Redis server. It
does not currently implement persistence, replication, pub/sub, clustering,
authentication, Lua scripting, or the full Redis command set.

## Implementation Highlights

- RESP request parsing and response encoding live in `redis_core`.
- Each command is implemented as its own `ICommand` class under
  `redis_core/commands/`, registered in a `CommandRegistry` that provides
  name-based lookup.
- `RedisExecutor` is a coordinator: it validates arguments, performs
  registry lookup, and delegates execution. Blocking and transaction state are
  owned by `IBlockingService` and `ITransactionService`.
- RESP reply types and reply encoding live in `redis_core/commands/reply-builder.*`.
- The in-memory data store lives in `redis_storage/redis-store.*`.
- Stream ID parsing, range lookup, and stream reads live in
  `redis_storage/redis-stream.*`.
- A compact byte-oriented radix trie with lexicographic ordering and
  inclusive-range traversal lives in `redis_storage/radix-trie.*`.
- The TCP server and `epoll` event loop live in `redis_net`.

## Project Structure

```text
src/
  main.cpp                 # Application entry point

  redis_core/
    resp-decoder.*         # RESP request parser
    resp-encoder.*         # RESP response encoder
    redis-command.*        # Command representation
    redis-executor.*       # Command coordinator (validation, lookup, dispatch)
    command-context.h      # Per-client execution context

    commands/
      command-interface.h  # ICommand, RedisReply, CommandDeps
      command-registry.*   # Name -> command registry and built-in registration
      reply-builder.*      # RESP reply types and encoding helpers
      *-command.*          # One class per supported command

    services/
      blocking-manager.*   # Blocking client tracking and timeouts
      blocking-service.h   # IBlockingService interface
      transaction-manager.*# Transaction queues and watched-key tracking
      transaction-service.h# ITransactionService interface

  redis_net/
    event-loop.*           # epoll-based event loop
    tcp-server.*           # TCP server setup and accept loop
    tcp-connection.*       # Per-client connection handling

  redis_storage/
    redis-store.*          # In-memory Redis data store
    redis-stream.*         # Redis stream implementation
    radix-trie.*           # Byte-oriented radix trie (lexicographic traversal)
```
