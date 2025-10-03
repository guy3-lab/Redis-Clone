# Redis Server Implementation

A fully-functional Redis server built from scratch in C++17, implementing the Redis Serialization Protocol (RESP) and supporting replication, persistence, pub/sub messaging, and geospatial indexing.

## Overview

This project demonstrates low-level systems programming by reimplementing core Redis functionality without using any Redis libraries. It handles binary protocols, concurrent client connections, and implements distributed systems concepts like master-replica replication.

**Lines of Code**: ~3,500+ across multiple modules  
**Development Time**: Independent project  
**Test Coverage**: 70+ comprehensive integration tests

## Key Features

### Core Data Structures
- **Strings**: GET, SET with expiry (PX flag)
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, blocking variants (BLPOP, BRPOP)
- **Streams**: XADD, XRANGE, XREAD with blocking support
- **Sorted Sets**: ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM

### Distributed Systems Features
- **Master-Replica Replication**
  - Automatic handshake (PING, REPLCONF, PSYNC)
  - Command propagation with offset tracking
  - WAIT command for synchronization guarantees
  - Full resync with RDB transfer
  
- **RDB Persistence**
  - Binary file format parser
  - Expiry timestamp handling (milliseconds & seconds)
  - Lazy expiration on key access

### Advanced Features
- **Pub/Sub Messaging**: Channel-based publish-subscribe with subscriber isolation
- **Transactions**: MULTI/EXEC/DISCARD with command queuing
- **Geospatial Commands**: GEOADD, GEOPOS, GEODIST, GEOSEARCH
  - 52-bit geohash encoding using bit interleaving
  - Haversine distance calculation
  - Radius-based search with multiple units (m, km, mi, ft)

## Technical Implementation

### Architecture

```
├── protocol/          RESP parser and encoder
├── storage/          In-memory data structures (strings, lists, streams, sorted sets)
├── replication/      Master-replica synchronization
├── rdb/             Binary file parser
├── blocking/         Client blocking for BLPOP/BRPOP/XREAD
├── pubsub/          Publisher-subscriber messaging
├── geo/             Geospatial indexing and search
└── commands/        Command handlers and routing
```

### Concurrency & Thread Safety
- **Multi-threaded**: One thread per client connection
- **Mutex protection**: All shared data structures protected by mutexes
- **Atomic operations**: Replication offset tracking uses atomics
- **Lock-free reads**: Optimized for read-heavy workloads where possible

### Networking
- **POSIX sockets**: TCP/IP networking with SO_REUSEADDR
- **Non-blocking I/O**: select() for pub/sub message delivery
- **Connection pooling**: Handles multiple concurrent clients
- **TCP_NODELAY**: Low-latency replication stream

### Key Algorithms

**Geohashing (52-bit)**
```cpp
// Interleaves 26 bits of longitude and 26 bits of latitude
// Uses binary subdivision for O(1) encoding/decoding
long long encode_geohash(double lon, double lat);
GeoCoordinates decode_geohash(long long hash);
```

**Haversine Distance**
```cpp
// Calculates great-circle distance between two points
// Uses Earth radius = 6372797.560856m (Redis standard)
double calculate_distance(double lon1, double lat1, double lon2, double lat2);
```

**RESP Protocol Parser**
```cpp
// Parses Redis wire protocol from TCP stream
// Handles arrays, bulk strings, integers, simple strings, errors
vector<string> parse_array_command(string& buffer);
```

## Building & Running

### Build
```bash
g++ -std=c++17 -pthread -O2 -o Server Server_all.cpp
```

### Run as Master
```bash
./Server --port 6379
```

### Run as Replica
```bash
./Server --port 6380 --replicaof "localhost 6379"
```

### With Persistence
```bash
./Server --port 6379 --dir /tmp/redis --dbfilename dump.rdb
```

## Testing

Comprehensive test suites covering all features:

```bash
# Compile all tests
g++ -std=c++17 -o test-replication tests/test_replication.cpp -pthread
g++ -std=c++17 -o test-rdb tests/test_rdb.cpp -pthread
g++ -std=c++17 -o test-pubsub tests/test_pubsub.cpp -pthread
g++ -std=c++17 -o test-sorted-sets tests/test_sorted_sets.cpp -pthread
g++ -std=c++17 -o test-geo tests/test_geo.cpp -pthread

# Run tests
./test-replication  # 16/16 tests
./test-rdb         # 17/17 tests
./test-pubsub      # 17/17 tests
./test-sorted-sets # 8/8 tests
./test-geo         # 16/16 tests
```

Total: **74 passing integration tests**

## Example Usage

```bash
# Terminal 1: Start server
./Server

# Terminal 2: Use redis-cli
redis-cli SET user:1 "Alice"
redis-cli GET user:1

# Pub/Sub
redis-cli SUBSCRIBE notifications
# (In another terminal)
redis-cli PUBLISH notifications "Hello World"

# Geospatial
redis-cli GEOADD cities -0.1278 51.5074 London
redis-cli GEOADD cities 2.2945 48.8584 Paris
redis-cli GEODIST cities London Paris km
# Output: 343.556
```

## Technical Challenges Solved

1. **Protocol Parsing**: Implementing a streaming parser for RESP that handles partial reads and maintains state across TCP packets

2. **Replication Synchronization**: Tracking byte offsets across master and replicas, handling the GETACK/ACK protocol for WAIT command guarantees

3. **Blocking Operations**: Implementing client suspension and wake-up notification for BLPOP/BRPOP without busy-waiting

4. **Thread Safety**: Avoiding deadlocks while maintaining consistency across concurrent operations on shared data structures

5. **Geohash Algorithm**: Implementing 52-bit geohash with proper bit interleaving for spatial indexing with minimal precision loss

## What I Learned

- Low-level network programming with POSIX sockets
- Binary protocol design and implementation
- Distributed systems concepts (replication, consistency, offset tracking)
- Multi-threaded programming with mutexes and condition variables
- Memory-efficient data structure design for in-memory databases
- Test-driven development for systems programming

## Performance Characteristics

- **Throughput**: Handles 10,000+ operations/second on single core
- **Latency**: Sub-millisecond response time for simple operations
- **Memory**: O(n) storage where n = total data size
- **Scalability**: Linear scaling with number of CPU cores (one thread per client)

## Project Context

Built as part of the [CodeCrafters](https://codecrafters.io) "Build Your Own Redis" challenge, extending beyond the base requirements to implement additional Redis features and create comprehensive test coverage.

## Technologies

- **C++17**: Modern C++ with STL containers
- **POSIX Threads**: pthread library for concurrency
- **Sockets**: TCP/IP networking with BSD sockets API
- **STL**: unordered_map, set, deque, vector for data structures

## Future Enhancements

- Persistence: Append-Only File (AOF) support
- Clustering: Redis Cluster protocol with hash slots
- Performance: Lock-free data structures using atomics
- Eviction: LRU/LFU cache eviction policies

---

*This project demonstrates proficiency in systems programming, network protocols, concurrent programming, and distributed systems design.*
