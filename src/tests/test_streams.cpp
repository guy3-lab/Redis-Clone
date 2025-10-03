#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <cassert>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <regex>

class RedisClient {
public:
    int sock;
    
public:
    RedisClient() : sock(-1) {}
    
    ~RedisClient() {
        if (sock >= 0) close(sock);
    }
    
    bool connect(int port = 6379) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return false;
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        
        if (::connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(sock);
            sock = -1;
            return false;
        }
        return true;
    }
    
    std::string send_command(const std::vector<std::string>& args) {
        std::string cmd = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto& arg : args) {
            cmd += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
        }
        
        send(sock, cmd.c_str(), cmd.size(), 0);
        
        char buffer[4096];
        memset(buffer, 0, sizeof(buffer));
        int bytes = recv(sock, buffer, sizeof(buffer), 0);
        
        if (bytes > 0) {
            return std::string(buffer, bytes);
        }
        return "";
    }
    
    std::string recv_with_timeout(int timeout_ms) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(sock, &readfds);
        
        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        
        int ready = select(sock + 1, &readfds, NULL, NULL, &tv);
        if (ready > 0) {
            char buffer[4096];
            memset(buffer, 0, sizeof(buffer));
            int bytes = recv(sock, buffer, sizeof(buffer), 0);
            if (bytes > 0) {
                return std::string(buffer, bytes);
            }
        }
        return "";
    }
};

bool test_passed(const std::string& test_name) {
    std::cout << "✓ " << test_name << " passed" << std::endl;
    return true;
}

bool test_failed(const std::string& test_name, const std::string& reason) {
    std::cout << "✗ " << test_name << " failed: " << reason << std::endl;
    return false;
}

bool test_type_command() {
    std::cout << "\n=== Testing TYPE command ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("TYPE", "Could not connect to server");
    }
    
    // Test string type
    client.send_command({"SET", "mykey", "myvalue"});
    auto resp = client.send_command({"TYPE", "mykey"});
    if (resp != "+string\r\n") {
        return test_failed("TYPE string", "Expected +string\\r\\n, got: " + resp);
    }
    
    // Test none type
    resp = client.send_command({"TYPE", "nonexistent"});
    if (resp != "+none\r\n") {
        return test_failed("TYPE none", "Expected +none\\r\\n, got: " + resp);
    }
    
    // Test stream type
    client.send_command({"XADD", "mystream", "0-1", "field", "value"});
    resp = client.send_command({"TYPE", "mystream"});
    if (resp != "+stream\r\n") {
        return test_failed("TYPE stream", "Expected +stream\\r\\n, got: " + resp);
    }
    
    return test_passed("TYPE");
}

bool test_xadd_explicit() {
    std::cout << "\n=== Testing XADD with explicit IDs ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("XADD explicit", "Could not connect to server");
    }
    
    // Add first entry
    auto resp = client.send_command({"XADD", "stream1", "0-1", "foo", "bar"});
    if (resp != "$3\r\n0-1\r\n") {
        return test_failed("XADD first", "Expected 0-1, got: " + resp);
    }
    
    // Add second entry with valid ID
    resp = client.send_command({"XADD", "stream1", "0-2", "bar", "baz"});
    if (resp != "$3\r\n0-2\r\n") {
        return test_failed("XADD second", "Expected 0-2, got: " + resp);
    }
    
    // Try invalid ID (equal)
    resp = client.send_command({"XADD", "stream1", "0-2", "test", "test"});
    if (!(resp.size() >= 4 && resp.compare(0, 4, "-ERR") == 0)) {
        return test_failed("XADD invalid equal", "Should reject equal ID");
    }
    
    // Try invalid ID (smaller)
    resp = client.send_command({"XADD", "stream1", "0-1", "test", "test"});
    if (!(resp.size() >= 4 && resp.compare(0, 4, "-ERR") == 0)) {
        return test_failed("XADD invalid smaller", "Should reject smaller ID");
    }
    
    // Try 0-0 (always invalid)
    resp = client.send_command({"XADD", "stream2", "0-0", "test", "test"});
    if (resp != "-ERR The ID specified in XADD must be greater than 0-0\r\n") {
        return test_failed("XADD 0-0", "Should reject 0-0 ID");
    }
    
    return test_passed("XADD explicit");
}

bool test_xadd_auto_seq() {
    std::cout << "\n=== Testing XADD with auto sequence ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("XADD auto-seq", "Could not connect to server");
    }
    
    // Test 0-* (should be 0-1)
    auto resp = client.send_command({"XADD", "stream3", "0-*", "foo", "bar"});
    if (resp != "$3\r\n0-1\r\n") {
        return test_failed("XADD 0-*", "Expected 0-1, got: " + resp);
    }
    
    // Test 5-* (should be 5-0)
    resp = client.send_command({"XADD", "stream4", "5-*", "foo", "bar"});
    if (resp != "$3\r\n5-0\r\n") {
        return test_failed("XADD 5-*", "Expected 5-0, got: " + resp);
    }
    
    // Test 5-* again (should be 5-1)
    resp = client.send_command({"XADD", "stream4", "5-*", "bar", "baz"});
    if (resp != "$3\r\n5-1\r\n") {
        return test_failed("XADD 5-* again", "Expected 5-1, got: " + resp);
    }
    
    return test_passed("XADD auto-seq");
}

bool test_xadd_auto_full() {
    std::cout << "\n=== Testing XADD with full auto (*) ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("XADD auto", "Could not connect to server");
    }
    
    auto resp = client.send_command({"XADD", "stream5", "*", "foo", "bar"});
    
    // Should return a timestamp-based ID
    if (!(resp.size() >= 1 && resp[0] == '$')) {
        return test_failed("XADD *", "Invalid response format");
    }
    
    // Extract the ID
    size_t start = resp.find("\r\n") + 2;
    size_t end = resp.find("\r\n", start);
    std::string id = resp.substr(start, end - start);
    
    // Verify it's a valid timestamp ID
    if (id.find('-') == std::string::npos) {
        return test_failed("XADD *", "ID should contain dash");
    }
    
    return test_passed("XADD auto");
}

bool test_xrange() {
    std::cout << "\n=== Testing XRANGE ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("XRANGE", "Could not connect to server");
    }
    
    // Add some entries
    client.send_command({"XADD", "xrange_test", "0-1", "a", "1"});
    client.send_command({"XADD", "xrange_test", "0-2", "b", "2"});
    client.send_command({"XADD", "xrange_test", "0-3", "c", "3"});
    
    // Test range query
    auto resp = client.send_command({"XRANGE", "xrange_test", "0-2", "0-3"});
    
    // Should contain 2 entries
    if (!(resp.size() >= 4 && resp.compare(0, 4, "*2\r\n") == 0)) {
        return test_failed("XRANGE", "Expected 2 entries");
    }
    
    // Test with - (from beginning)
    resp = client.send_command({"XRANGE", "xrange_test", "-", "0-2"});
    if (!(resp.size() >= 4 && resp.compare(0, 4, "*2\r\n") == 0)) {
        return test_failed("XRANGE -", "Expected 2 entries from beginning");
    }
    
    // Test with + (to end)
    resp = client.send_command({"XRANGE", "xrange_test", "0-2", "+"});
    if (!(resp.size() >= 4 && resp.compare(0, 4, "*2\r\n") == 0)) {
        return test_failed("XRANGE +", "Expected 2 entries to end");
    }
    
    return test_passed("XRANGE");
}

bool test_xread() {
    std::cout << "\n=== Testing XREAD ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("XREAD", "Could not connect to server");
    }
    
    // Add an entry
    client.send_command({"XADD", "xread_test", "0-1", "field", "value"});
    
    // Read after 0-0
    auto resp = client.send_command({"XREAD", "STREAMS", "xread_test", "0-0"});
    
    // Should return the entry
    if (!(resp.size() >= 4 && resp.compare(0, 4, "*1\r\n") == 0)) {
        return test_failed("XREAD", "Expected 1 stream in response");
    }
    
    // Read after the last entry (should be empty)
    resp = client.send_command({"XREAD", "STREAMS", "xread_test", "0-1"});
    // XREAD returns null array when no data
    if (resp != "*-1\r\n") {
        return test_failed("XREAD empty", "Expected null array response, got: " + resp);
    }
    
    return test_passed("XREAD");
}

bool test_xread_blocking() {
    std::cout << "\n=== Testing XREAD BLOCK ===" << std::endl;
    
    RedisClient blocker;
    if (!blocker.connect()) {
        return test_failed("XREAD BLOCK", "Could not connect blocker");
    }
    
    RedisClient pusher;
    if (!pusher.connect()) {
        return test_failed("XREAD BLOCK", "Could not connect pusher");
    }
    
    // Add initial entry
    pusher.send_command({"XADD", "block_test", "0-1", "initial", "data"});
    
    // Send blocking XREAD
    std::string block_cmd = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$1\r\n0\r\n$7\r\nSTREAMS\r\n$10\r\nblock_test\r\n$3\r\n0-1\r\n";
    send(blocker.sock, block_cmd.c_str(), block_cmd.size(), 0);
    
    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Add new entry
    pusher.send_command({"XADD", "block_test", "0-2", "new", "data"});
    
    // Blocker should receive response
    auto resp = blocker.recv_with_timeout(1000);
    if (!(resp.size() >= 4 && resp.compare(0, 4, "*1\r\n") == 0)) {
        return test_failed("XREAD BLOCK", "Should receive new entry");
    }
    
    return test_passed("XREAD BLOCK");
}

bool test_xread_timeout() {
    std::cout << "\n=== Testing XREAD BLOCK timeout ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("XREAD timeout", "Could not connect");
    }
    
    auto start = std::chrono::steady_clock::now();
    
    // Block for 100ms on empty stream
    auto resp = client.send_command({"XREAD", "BLOCK", "100", "STREAMS", "timeout_test", "0-0"});
    
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    // XREAD returns null array on timeout
    if (resp != "*-1\r\n") {
        return test_failed("XREAD timeout", "Expected null array on timeout, got: " + resp);
    }
    
    if (elapsed < 90 || elapsed > 150) {
        return test_failed("XREAD timeout duration", "Incorrect timeout: " + std::to_string(elapsed) + "ms");
    }
    
    return test_passed("XREAD timeout");
}

bool test_xread_dollar() {
    std::cout << "\n=== Testing XREAD with $ ===" << std::endl;
    
    RedisClient blocker;
    if (!blocker.connect()) {
        return test_failed("XREAD $", "Could not connect blocker");
    }
    
    RedisClient pusher;
    if (!pusher.connect()) {
        return test_failed("XREAD $", "Could not connect pusher");
    }
    
    // Add initial entry
    pusher.send_command({"XADD", "dollar_test", "0-1", "old", "data"});
    
    // Block with $ (only new entries)
    std::string block_cmd = "*6\r\n$5\r\nXREAD\r\n$5\r\nBLOCK\r\n$3\r\n500\r\n$7\r\nSTREAMS\r\n$11\r\ndollar_test\r\n$1\r\n$\r\n";
    send(blocker.sock, block_cmd.c_str(), block_cmd.size(), 0);
    
    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Add new entry
    pusher.send_command({"XADD", "dollar_test", "0-2", "new", "data"});
    
    // Should receive only the new entry
    auto resp = blocker.recv_with_timeout(1000);
    if (!(resp.size() >= 4 && resp.compare(0, 4, "*1\r\n") == 0)) {
        return test_failed("XREAD $", "Should receive new entry");
    }
    
    return test_passed("XREAD $");
}

int main() {
    std::cout << "=====================================" << std::endl;
    std::cout << "    Redis Streams Test Suite" << std::endl;
    std::cout << "=====================================" << std::endl;
    
    int passed = 0;
    int total = 0;
    
    std::vector<std::pair<std::string, bool(*)()>> tests = {
        {"TYPE command", test_type_command},
        {"XADD explicit IDs", test_xadd_explicit},
        {"XADD auto sequence", test_xadd_auto_seq},
        {"XADD full auto", test_xadd_auto_full},
        {"XRANGE", test_xrange},
        {"XREAD", test_xread},
        {"XREAD BLOCK", test_xread_blocking},
        {"XREAD timeout", test_xread_timeout},
        {"XREAD with $", test_xread_dollar}
    };
    
    for (const auto& [name, test_func] : tests) {
        total++;
        if (test_func()) {
            passed++;
        }
    }
    
    std::cout << "\n=====================================" << std::endl;
    std::cout << "Results: " << passed << "/" << total << " tests passed" << std::endl;
    
    if (passed == total) {
        std::cout << "All tests passed" << std::endl;
        return 0;
    } else {
        std::cout << "One or more tests failed" << std::endl;
        return 1;
    }
}