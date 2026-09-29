# Redis Server from Scratch

A Redis-compatible server written in C++17, featuring master-replica replication, RDB loading, pub/sub messaging, streams, and geospatial indexing. Built on raw POSIX sockets and the standard library, with no external dependencies, to go deep on systems programming, network protocols, and distributed systems.

## Core Capabilities

### Data Structures
- **Key-Value Store**: SET (with `PX` millisecond expiry), GET, INCR, TYPE, KEYS `*`
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN + blocking pops (BLPOP, BRPOP) with fractional-second timeouts
- **Streams**: XADD with auto-generated IDs, XRANGE, XREAD with `COUNT`, `BLOCK`, multiple streams, and `$`
- **Sorted Sets**: ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM, with equal scores ordered lexicographically by member
- **Transactions**: MULTI, EXEC, DISCARD; EXEC runs the queued commands back-to-back while holding the transaction lock

### Distributed Systems Features

**Master-Replica Replication**
- Full handshake protocol (PING, REPLCONF, PSYNC)
- Live command propagation to multiple replicas
- Byte offset tracking on master and replicas
- WAIT: sends `REPLCONF GETACK` and counts replicas whose acknowledged offset has reached the last write, with a timeout
- Replicas reconnect to the master automatically if the connection drops

**RDB Persistence**
- Binary RDB parser: header, metadata, database selector, resize hints, and expiry opcodes
- Length-encoded and integer-encoded strings
- Millisecond and second expiry timestamps
- Lazy expiration on access

**Pub/Sub Messaging**
- Channel-based publish/subscribe
- Multi-channel subscriptions per client
- Subscribe mode isolation (restricts which commands a subscribed client can run)
- Automatic cleanup on client disconnect

**Geospatial Indexing**
- 52-bit geohash encoding via bit interleaving
- Coordinate validation against Redis's Web Mercator bounds
- Haversine distance (GEODIST) and radius search (GEOSEARCH) in m, km, mi, or ft

**Concurrency Model**
- **Thread-per-connection**: one thread per client, benchmarked up to 200 concurrent clients
- **Coarse-grained locking**: a single storage mutex guards all data, held only for the in-memory operation
- **Atomic offsets**: replication and ACK offsets are `std::atomic`
- **Condition variables for blocking reads**, no polling threads:
  - BLPOP/BRPOP: each waiter sleeps on its own condition variable. LPUSH/RPUSH pop the element for the oldest waiter, then wake only that thread, so clients are served in the order they blocked
  - XREAD BLOCK: waiters share one condition variable that XADD broadcasts on, since reads don't consume entries
  - The check for data and the wait both happen under the storage mutex, so a write can't slip in between them and be missed
- **Opt-in debug tracing**: set `REDIS_DEBUG=1` to log every command to stderr. It's off by default because synchronous logging was the main throughput bottleneck (see below)

**Network Protocol**
- **RESP parser**: incremental parser for the Redis Serialization Protocol
- **Partial read handling**: buffers incomplete messages across TCP reads
- **Pipelining**: multiple commands per read are parsed and executed in order
- **TCP_NODELAY**: Nagle's algorithm disabled on replica connections for low-latency propagation


## Performance

Measured on an Apple M4 Pro (12 cores) over localhost with `redis-benchmark` and small Python scripts that time how long blocked clients take to wake. Throughput rows are from 2–3 runs each; latency rows are 200–500 samples each.

**Throughput** (`redis-benchmark -t set,get -n 100000`)

| Setup | SET ops/sec | GET ops/sec |
|-------|-------------|-------------|
| 10 / 50 / 200 clients, no pipelining | 138–152k | 136–151k |
| 50 clients, pipelined (`-P 16`) | ~1.03M | ~1.02M |
| 50 clients, 3 replicas attached | ~60k | — |
| Real Redis, same machine, 50 clients | 171k | 182k |

Throughput stays flat from 10 to 200 clients. With replicas attached, SET slows down because each write is forwarded to every replica before the client gets its reply.

**Latency**

| Measurement | p50 | p99 |
|-------------|-----|-----|
| BLPOP wake-up after RPUSH | 0.39 ms | 1.6 ms |
| XREAD BLOCK wake-up after XADD | 0.41 ms | 1.5 ms |
| SET + `WAIT 3` with 3 replicas | 12.6 ms | 13.9 ms |

In the WAIT test, every call got acknowledgments from all 3 replicas (400 of 400). After 200k random-key SETs, the master and all 3 replicas held identical key counts (86,646 keys each).

**What changed along the way**

| Change | Before | After |
|--------|--------|-------|
| Made debug logging opt-in (it wrote unbuffered stderr on every command, serializing all threads) | ~17k SET / ~30k GET ops/sec at any client count | 136–152k ops/sec |
| Replaced 10 ms polling loops with condition variables | BLPOP wake-up p50 5.6 ms, p99 13.1 ms | p50 0.39 ms, p99 1.6 ms |
| Same change, idle cost | 31.5% of a core with 200 clients blocked | ~0% |

Reproduce the throughput numbers with (after `./run_tests.sh` has built the server):

```bash
./build/tests/Server --port 6379 &
redis-benchmark -p 6379 -t set,get -n 100000 -c 50 -q
```


## Limitations

- Initial sync sends an empty RDB snapshot, so a replica only receives writes made after it connects
- WAIT checks for acknowledgments every 10 ms, which accounts for most of its latency
- The RDB loader handles string values only, and `KEYS` supports only the `*` pattern
- SET supports `PX` expiry but not `EX`, `NX`, or `XX`
- A client that disconnects while blocked isn't noticed until data arrives for its key or its timeout expires


## Quality & Testing

**8 test suites** with 110 integration tests, all passing. Each test talks to a live server process over TCP. The replication suite starts separate master and replica processes.

| Test Suite | Tests | Coverage |
|------------|-------|----------|
| Lists & Blocking | 10 | LPUSH, RPUSH, BLPOP, BRPOP, timeouts, FIFO order across blocked clients |
| Transactions | 18 | MULTI/EXEC/DISCARD, error handling, isolation |
| Streams | 9 | XADD, XRANGE, XREAD, XREAD BLOCK, `$` |
| Replication | 16 | Handshake, propagation, WAIT, recovery |
| RDB Persistence | 16 | Loading, expiry, CONFIG GET, KEYS |
| Pub/Sub | 17 | SUBSCRIBE, PUBLISH, multi-channel |
| Sorted Sets | 8 | ZADD, ZRANK, ZRANGE, scoring |
| Geo Commands | 16 | GEOADD, GEOPOS, GEODIST, GEOSEARCH |


## Lessons Learned

- How TCP's stream-based nature affects application protocols
- Why offset tracking is critical for distributed system consistency
- The tradeoffs between different data structure implementations
- How to debug multi-threaded race conditions with strategic logging
- That the same logging can become the bottleneck: flat throughput as clients increase means something is serializing every thread
- Why blocking list pops need a FIFO hand-off to one waiter while stream reads can simply wake everyone
- Why binary protocols are more efficient than text protocols


## Build & Run

```bash
# Compile server (from the repo root)
c++ -std=c++17 -pthread -O2 -o Server src/Server_all.cpp

# Run as standalone server
./Server --port 6379

# Run as replica
./Server --port 6380 --replicaof "localhost 6379"

# Run with an RDB file
./Server --port 6379 --dir /tmp/redis --dbfilename dump.rdb

# Run with debug tracing on stderr
REDIS_DEBUG=1 ./Server --port 6379
```

## Testing

```bash
# Build the server and all 8 suites into build/tests, then run them (110 tests)
./run_tests.sh
```

## Example Session

```bash
# Terminal 1: Master server
./Server --port 6379

# Terminal 2: Replica server  
./Server --port 6380 --replicaof "localhost 6379"

# Terminal 3: Client operations
$ redis-cli -p 6379

# Basic operations
> SET user:1000 "Alice"
OK
> GET user:1000
"Alice"

# Lists with blocking
> BLPOP queue 5
(blocks for 5 seconds or until data available)

# Pub/Sub
> SUBSCRIBE notifications
1) "subscribe"
2) "notifications"  
3) (integer) 1

# Geospatial
> GEOADD cities 2.2945 48.8584 Paris -0.1278 51.5074 London
(integer) 2
> GEODIST cities Paris London km
"341.386918"
> GEOSEARCH cities FROMLONLAT 0 50 BYRADIUS 500 km
1) "London"
2) "Paris"

# Verify replication
$ redis-cli -p 6380 GET user:1000
"Alice"  # Data replicated!
```

---

**Built as part of CodeCrafters "Build Your Own Redis" challenge**  
