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
    
    void disconnect() {
        if (sock >= 0) {
            close(sock);
            sock = -1;
        }
    }
    
    std::string send_raw(const std::string& cmd) {
        if (sock < 0) return "";
        send(sock, cmd.c_str(), cmd.size(), 0);
        
        char buffer[4096];
        memset(buffer, 0, sizeof(buffer));
        int bytes = recv(sock, buffer, sizeof(buffer), 0);
        
        if (bytes > 0) {
            return std::string(buffer, bytes);
        }
        return "";
    }
    
    std::string send_command(const std::vector<std::string>& args) {
        std::string cmd = "*" + std::to_string(args.size()) + "\r\n";
        for (const auto& arg : args) {
            cmd += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
        }
        return send_raw(cmd);
    }
    
    // Non-blocking receive with timeout
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

// Test helper functions
bool test_passed(const std::string& test_name) {
    std::cout << "✓ " << test_name << " passed" << std::endl;
    return true;
}

bool test_failed(const std::string& test_name, const std::string& reason) {
    std::cout << "✗ " << test_name << " failed: " << reason << std::endl;
    return false;
}

// Test cases
bool test_lpush_rpush() {
    std::cout << "\n=== Testing LPUSH/RPUSH ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("LPUSH/RPUSH", "Could not connect to server");
    }
    
    // Test RPUSH
    auto resp = client.send_command({"RPUSH", "mylist", "a", "b", "c"});
    if (resp != ":3\r\n") {
        return test_failed("RPUSH", "Expected :3\\r\\n, got: " + resp);
    }
    
    // Test LPUSH
    resp = client.send_command({"LPUSH", "mylist", "d"});
    if (resp != ":4\r\n") {
        return test_failed("LPUSH", "Expected :4\\r\\n, got: " + resp);
    }
    
    return test_passed("LPUSH/RPUSH");
}

bool test_lrange() {
    std::cout << "\n=== Testing LRANGE ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("LRANGE", "Could not connect to server");
    }
    
    // Setup list
    client.send_command({"RPUSH", "rangelist", "1", "2", "3", "4", "5"});
    
    // Test full range
    auto resp = client.send_command({"LRANGE", "rangelist", "0", "-1"});
    if (resp.find("*5\r\n") != 0) {
        return test_failed("LRANGE full", "Expected array of 5 elements");
    }
    
    // Test partial range
    resp = client.send_command({"LRANGE", "rangelist", "1", "3"});
    if (resp.find("*3\r\n") != 0) {
        return test_failed("LRANGE partial", "Expected array of 3 elements");
    }
    
    // Test empty list
    resp = client.send_command({"LRANGE", "nonexistent", "0", "-1"});
    if (resp != "*0\r\n") {
        return test_failed("LRANGE empty", "Expected *0\\r\\n for nonexistent list");
    }
    
    return test_passed("LRANGE");
}

bool test_llen() {
    std::cout << "\n=== Testing LLEN ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("LLEN", "Could not connect to server");
    }
    
    // Test empty list
    auto resp = client.send_command({"LLEN", "newlist"});
    if (resp != ":0\r\n") {
        return test_failed("LLEN empty", "Expected :0\\r\\n, got: " + resp);
    }
    
    // Add items and test
    client.send_command({"RPUSH", "newlist", "a", "b", "c"});
    resp = client.send_command({"LLEN", "newlist"});
    if (resp != ":3\r\n") {
        return test_failed("LLEN", "Expected :3\\r\\n, got: " + resp);
    }
    
    return test_passed("LLEN");
}

bool test_lpop_rpop() {
    std::cout << "\n=== Testing LPOP/RPOP ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("LPOP/RPOP", "Could not connect to server");
    }
    
    // Setup list
    client.send_command({"RPUSH", "poplist", "a", "b", "c", "d"});
    
    // Test LPOP
    auto resp = client.send_command({"LPOP", "poplist"});
    if (resp != "$1\r\na\r\n") {
        return test_failed("LPOP", "Expected $1\\r\\na\\r\\n, got: " + resp);
    }
    
    // Test RPOP
    resp = client.send_command({"RPOP", "poplist"});
    if (resp != "$1\r\nd\r\n") {
        return test_failed("RPOP", "Expected $1\\r\\nd\\r\\n, got: " + resp);
    }
    
    // Test LPOP with count
    resp = client.send_command({"LPOP", "poplist", "2"});
    if (resp != "*2\r\n$1\r\nb\r\n$1\r\nc\r\n") {
        return test_failed("LPOP count", "Expected array with b and c");
    }
    
    // Test on empty list
    resp = client.send_command({"LPOP", "poplist"});
    if (resp != "$-1\r\n") {
        return test_failed("LPOP empty", "Expected $-1\\r\\n for empty list");
    }
    
    return test_passed("LPOP/RPOP");
}

bool test_blpop_immediate() {
    std::cout << "\n=== Testing BLPOP (immediate) ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("BLPOP immediate", "Could not connect to server");
    }
    
    // Add element first
    client.send_command({"RPUSH", "quicklist", "value1"});
    
    // BLPOP should return immediately
    auto resp = client.send_command({"BLPOP", "quicklist", "0"});
    if (resp != "*2\r\n$9\r\nquicklist\r\n$6\r\nvalue1\r\n") {
        return test_failed("BLPOP immediate", "Expected immediate response with value1");
    }
    
    return test_passed("BLPOP immediate");
}

bool test_blpop_timeout() {
    std::cout << "\n=== Testing BLPOP (timeout) ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("BLPOP timeout", "Could not connect to server");
    }
    
    auto start = std::chrono::steady_clock::now();
    
    // BLPOP with 0.1 second timeout on empty list
    auto resp = client.send_command({"BLPOP", "emptylist", "0.1"});
    
    auto end = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    if (resp != "$-1\r\n") {
        return test_failed("BLPOP timeout", "Expected $-1\\r\\n on timeout, got: " + resp);
    }
    
    if (elapsed < 90 || elapsed > 150) {
        return test_failed("BLPOP timeout", "Timeout duration incorrect: " + std::to_string(elapsed) + "ms");
    }
    
    return test_passed("BLPOP timeout");
}

bool test_blpop_blocking() {
    std::cout << "\n=== Testing BLPOP (blocking with wake) ===" << std::endl;
    
    RedisClient blocker;
    if (!blocker.connect()) {
        return test_failed("BLPOP blocking", "Could not connect blocker");
    }
    
    RedisClient pusher;
    if (!pusher.connect()) {
        return test_failed("BLPOP blocking", "Could not connect pusher");
    }
    
    // Send BLPOP command (blocks)
    std::string blpop_cmd = "*3\r\n$5\r\nBLPOP\r\n$9\r\nblocklist\r\n$1\r\n5\r\n";
    send(blocker.sock, blpop_cmd.c_str(), blpop_cmd.size(), 0);
    
    // Wait a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Push element from another client
    pusher.send_command({"RPUSH", "blocklist", "wakeup"});
    
    // Blocker should receive response
    auto resp = blocker.recv_with_timeout(1000);
    if (resp != "*2\r\n$9\r\nblocklist\r\n$6\r\nwakeup\r\n") {
        return test_failed("BLPOP blocking", "Expected wakeup response, got: " + resp);
    }
    
    return test_passed("BLPOP blocking");
}

bool test_blpop_multiple_clients() {
    std::cout << "\n=== Testing BLPOP (multiple clients) ===" << std::endl;
    
    RedisClient client1, client2, pusher;
    if (!client1.connect() || !client2.connect() || !pusher.connect()) {
        return test_failed("BLPOP multiple", "Could not connect clients");
    }
    
    // Both clients block on same list (client1 first)
    std::string blpop_cmd = "*3\r\n$5\r\nBLPOP\r\n$8\r\nmultikey\r\n$1\r\n5\r\n";
    send(client1.sock, blpop_cmd.c_str(), blpop_cmd.size(), 0);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    send(client2.sock, blpop_cmd.c_str(), blpop_cmd.size(), 0);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // Push one element
    pusher.send_command({"RPUSH", "multikey", "first"});
    
    // Client1 should get it (was waiting longer)
    auto resp1 = client1.recv_with_timeout(500);
    if (resp1.find("first") == std::string::npos) {
        return test_failed("BLPOP multiple", "Client1 should receive first element");
    }
    
    // Client2 should still be blocked
    auto resp2 = client2.recv_with_timeout(100);
    if (!resp2.empty()) {
        return test_failed("BLPOP multiple", "Client2 should still be blocked");
    }
    
    // Push another element for client2
    pusher.send_command({"RPUSH", "multikey", "second"});
    resp2 = client2.recv_with_timeout(500);
    if (resp2.find("second") == std::string::npos) {
        return test_failed("BLPOP multiple", "Client2 should receive second element");
    }
    
    return test_passed("BLPOP multiple clients");
}

bool test_blpop_multiple_keys() {
    std::cout << "\n=== Testing BLPOP (multiple keys) ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("BLPOP multi-key", "Could not connect");
    }
    
    // BLPOP on multiple keys, second one has data
    client.send_command({"RPUSH", "key2", "data"});
    auto resp = client.send_command({"BLPOP", "key1", "key2", "key3", "0"});
    
    if (resp != "*2\r\n$4\r\nkey2\r\n$4\r\ndata\r\n") {
        return test_failed("BLPOP multi-key", "Should pop from key2");
    }
    
    return test_passed("BLPOP multiple keys");
}

bool test_brpop() {
    std::cout << "\n=== Testing BRPOP ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("BRPOP", "Could not connect");
    }
    
    // Setup list
    client.send_command({"RPUSH", "brlist", "a", "b", "c"});
    
    // BRPOP should pop from right
    auto resp = client.send_command({"BRPOP", "brlist", "0"});
    if (resp != "*2\r\n$6\r\nbrlist\r\n$1\r\nc\r\n") {
        return test_failed("BRPOP", "Expected 'c' from right, got: " + resp);
    }
    
    return test_passed("BRPOP");
}

int main() {
    std::cout << "=====================================" << std::endl;
    std::cout << "    Redis Lists Test Suite" << std::endl;
    std::cout << "=====================================" << std::endl;
    
    int passed = 0;
    int total = 0;
    
    // Run all tests
    std::vector<std::pair<std::string, bool(*)()>> tests = {
        {"LPUSH/RPUSH", test_lpush_rpush},
        {"LRANGE", test_lrange},
        {"LLEN", test_llen},
        {"LPOP/RPOP", test_lpop_rpop},
        {"BLPOP immediate", test_blpop_immediate},
        {"BLPOP timeout", test_blpop_timeout},
        {"BLPOP blocking", test_blpop_blocking},
        {"BLPOP multiple clients", test_blpop_multiple_clients},
        {"BLPOP multiple keys", test_blpop_multiple_keys},
        {"BRPOP", test_brpop}
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