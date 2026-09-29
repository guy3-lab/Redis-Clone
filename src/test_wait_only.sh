#!/bin/bash

echo "Compiling..."
g++ -std=c++17 -pthread -O2 -o Server Server_all.cpp
g++ -std=c++17 -o test-replication tests/test_replication.cpp -pthread

echo "Running WAIT tests with filtered debug..."
REDIS_DEBUG=1 ./test-replication 2>&1 | grep -E "(WAIT_|ACK_TRACK|PSYNC_TRACK|REPLICA STREAM|Testing WAIT|passed|failed)" > wait_debug.txt

echo "Debug output saved to wait_debug.txt"
echo "Showing results:"
cat wait_debug.txt