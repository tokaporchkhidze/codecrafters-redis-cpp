# Redis Clone in C++

A small Redis-compatible server built in C++ as part of the CodeCrafters
"Build Your Own Redis" challenge.

The project implements a TCP server, RESP command parsing, command execution,
and an in-memory data store for strings, lists, and streams.

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
- Multiple client connections
- String commands:
   - `PING`
   - `ECHO`
   - `SET`
   - `GET`
   - `TYPE`
- Key expiry with `SET key value PX milliseconds`
- List commands:
   - `RPUSH`
   - `LPUSH`
   - `LLEN`
   - `LRANGE`
   - `LPOP`
   - `BLPOP`
- Stream commands:
   - `XADD`
   - `XRANGE`
   - `XREAD`

## Project Structure

```text
src/
  main.cpp                 # Application entry point

  redis_core/
    resp-decoder.*         # RESP request parser
    resp-encoder.*         # RESP response encoder
    redis-command.*        # Command representation
    redis-executor.*       # Command dispatch and execution

  redis_net/
    event-loop.*           # epoll-based event loop
    tcp-server.*           # TCP server setup and accept loop
    tcp-connection.*       # Per-client connection handling

  redis_storage/
    redis-store.*          # In-memory Redis data store
    redis-stream.*         # Redis stream implementation