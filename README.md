# Redis Server from Scratch

A production-grade Redis server implementation in C++17, featuring master-replica replication, RDB persistence, pub/sub messaging, and geospatial indexing. Built without external libraries to demonstrate depth in understanding of systems programming, network protocols, and distributed systems.

## Core Capabilities

### Data Structures
- **Key-Value Store**: SET, GET, INCR with millisecond-precision expiry
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN + blocking operations (BLPOP, BRPOP)
- **Streams**: XADD with auto-ID generation, XRANGE, XREAD with BLOCK support
- **Sorted Sets**: ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM with lexicographic ordering
- **Transactions**: MULTI, EXEC, DISCARD with atomic execution guarantees

### Distributed Systems Features

**Master-Replica Replication**
- Full handshake protocol (PING, REPLCONF, PSYNC)
- Real-time command propagation to multiple replicas
- Byte offset tracking for consistency verification
- WAIT command with acknowledgment timeout for durability guarantees
- RDB snapshot transfer for initial sync

**RDB Persistence**
- Binary file format parser compatible with Redis RDB v9
- Length-encoded strings and integer compression
- Millisecond/second expiry timestamp support
- Lazy expiration on access

**Pub/Sub Messaging**
- Channel-based publisher-subscriber architecture
- Multi-channel subscriptions per client
- Subscribe mode isolation (restricts command execution)
- Automatic cleanup on client disconnect

**Geospatial Indexing**
- 52-bit geohash encoding via bit interleaving
- Coordinate validation (Web Mercator projection bounds)
- Haversine distance calculation with Earth radius accuracy
- Radius-based search (GEOSEARCH) with multiple units

**Concurrency Model**
- **Thread-per-connection**: Dedicated thread for each client (scalable to thousands)
- **Mutex-protected state**: Fine-grained locking on data structures
- **Atomic offsets**: Lock-free replication offset tracking
- **Condition variables**: Efficient blocking for BLPOP/BRPOP without spinning
  
**Network Protocol**
- **RESP Parser**: Streaming parser for Redis Serialization Protocol
- **Partial read handling**: Buffers incomplete messages across TCP packets
- **Pipelining support**: Multiple commands in single TCP send
- **TCP_NODELAY**: Disabled Nagle algorithm for low-latency replication


## Quality & Testing

**8 robust test suites** with 111 integration tests:

| Test Suite | Tests | Coverage |
|------------|-------|----------|
| Lists & Blocking | 10 | LPUSH, RPUSH, BLPOP, BRPOP, timeouts |
| Transactions | 18 | MULTI/EXEC, error handling, isolation |
| Streams | 9 | XADD, XRANGE, XREAD blocking |
| Replication | 16 | Handshake, propagation, WAIT, recovery |
| RDB Persistence | 17 | Loading, expiry, CONFIG GET, KEYS |
| Pub/Sub | 17 | SUBSCRIBE, PUBLISH, multi-channel |
| Sorted Sets | 8 | ZADD, ZRANK, ZRANGE, scoring |
| Geo Commands | 16 | GEOADD, GEOPOS, GEODIST, GEOSEARCH |


## Build & Run

```bash
# Compile server
g++ -std=c++17 -pthread -O2 -o Server Server_all.cpp

# Run as standalone server
./Server --port 6379

# Run as replica
./Server --port 6380 --replicaof "localhost 6379"

# Run with persistence
./Server --port 6379 --dir /tmp/redis --dbfilename dump.rdb
```

## Testing

```bash
# Compile test suite
make tests  # or run individual test compilations

# Run all tests (110 tests)
./test-lists && ./test-transactions && ./test-streams && \
./test-replication && ./test-rdb && ./test-pubsub && \
./test-sorted-sets && ./test-geo

# Or create a test runner script
./run_all_tests.sh
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
"343.556"
> GEOSEARCH cities FROMLONLAT 0 50 BYRADIUS 500 km
1) "London"
2) "Paris"

# Verify replication
$ redis-cli -p 6380 GET user:1000
"Alice"  # Data replicated!
```


## Lessons Learned

- How TCP's stream-based nature affects application protocols
- Why offset tracking is critical for distributed system consistency
- The tradeoffs between different data structure implementations
- How to debug multi-threaded race conditions with strategic logging
- Why binary protocols are more efficient than text protocols

---

**Built as part of CodeCrafters "Build Your Own Redis" challenge**  
