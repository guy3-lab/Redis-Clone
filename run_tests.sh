#!/bin/bash
#
# Builds the server and all 8 test suites into build/tests, then runs them.
# The suites start ./Server themselves or talk to one on port 6379,
# so everything runs from that directory.

set -u
cd "$(dirname "$0")"

CXX="${CXX:-c++}"
OUT=build/tests
mkdir -p "$OUT"

echo "Building server..."
"$CXX" -std=c++17 -O2 -pthread -o "$OUT/Server" src/Server_all.cpp || exit 1

SUITES="lists streams transactions geo pubsub rdb replication sorted_sets"
for t in $SUITES; do
  echo "Building test-$t..."
  "$CXX" -std=c++17 -O2 -pthread -o "$OUT/test-$t" "src/tests/test_$t.cpp" || exit 1
done

cd "$OUT"

port_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

failed=0
for t in $SUITES; do
  server_pid=""
  # these suites expect a server already running on 6379, the rest start their own
  case $t in
    lists|streams|transactions)
      ./Server --port 6379 >/dev/null 2>&1 &
      server_pid=$!
      for _ in $(seq 1 50); do port_open 6379 && break; sleep 0.1; done
      ;;
  esac

  output=$(./test-$t 2>&1)
  status=$?
  result=$(echo "$output" | grep "Results:" | tail -1)
  echo "$t: ${result:-no results}"
  [ "$status" -ne 0 ] && failed=1

  [ -n "$server_pid" ] && kill "$server_pid" 2>/dev/null && wait "$server_pid" 2>/dev/null
  pkill -f "^./Server" 2>/dev/null
  sleep 0.5
done

exit $failed
