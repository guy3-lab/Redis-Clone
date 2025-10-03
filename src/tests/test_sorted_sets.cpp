#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <sstream>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>

class RedisClient {
public:
    int sock;
    
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
};

class RedisServer {
public:
    pid_t pid;
    int port;
    
    RedisServer(int p = 6379) : pid(-1), port(p) {}
    
    ~RedisServer() {
        stop();
    }
    
    bool start() {
        pid = fork();
        if (pid == 0) {
            freopen("/dev/null", "w", stdout);
            freopen("/dev/null", "w", stderr);
            execl("/bin/sh", "sh", "-c", ("./Server --port " + std::to_string(port)).c_str(), nullptr);
            exit(1);
        } else if (pid > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            return true;
        }
        return false;
    }
    
    void stop() {
        if (pid > 0) {
            kill(pid, SIGTERM);
            waitpid(pid, nullptr, 0);
            pid = -1;
        }
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

std::string escape_string(const std::string& s) {
    std::stringstream ss;
    for (char c : s) {
        if (c == '\r') ss << "\\r";
        else if (c == '\n') ss << "\\n";
        else ss << c;
    }
    return ss.str();
}

// ============= Stage 1: ZADD (Create Sorted Set) =============

bool test_zadd_create_sorted_set() {
    std::cout << "\n=== Testing ZADD create sorted set ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZADD create", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZADD create", "Could not connect");
    }
    
    // Add first member to new sorted set
    auto resp = client.send_command({"ZADD", "zset_key", "10.0", "zset_member"});
    
    // Should return :1\r\n (1 new member added)
    if (resp != ":1\r\n") {
        return test_failed("ZADD create", "Expected :1\\r\\n, got: " + escape_string(resp));
    }
    
    return test_passed("ZADD create sorted set");
}

// ============= Stage 2: ZADD (Add to Existing) =============

bool test_zadd_add_members() {
    std::cout << "\n=== Testing ZADD add members ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZADD add", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZADD add", "Could not connect");
    }
    
    // Create sorted set
    client.send_command({"ZADD", "zset_key", "20.0", "member1"});
    
    // Add new member
    auto resp = client.send_command({"ZADD", "zset_key", "30.1", "member2"});
    if (resp != ":1\r\n") {
        return test_failed("ZADD add new", "Expected :1\\r\\n for new member");
    }
    
    // Update existing member (should return 0)
    resp = client.send_command({"ZADD", "zset_key", "100.0", "member1"});
    if (resp != ":0\r\n") {
        return test_failed("ZADD update", "Expected :0\\r\\n for update, got: " + escape_string(resp));
    }
    
    return test_passed("ZADD add members");
}

// ============= Stage 3: ZRANK =============

bool test_zrank_command() {
    std::cout << "\n=== Testing ZRANK command ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZRANK", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZRANK", "Could not connect");
    }
    
    // Add members with different scores
    client.send_command({"ZADD", "zset_key", "100.0", "foo"});
    client.send_command({"ZADD", "zset_key", "100.0", "bar"});
    client.send_command({"ZADD", "zset_key", "20.0", "baz"});
    client.send_command({"ZADD", "zset_key", "30.1", "caz"});
    
    // Expected ranks: baz=0, caz=1, bar=2 (lexicographic), foo=3
    
    auto resp = client.send_command({"ZRANK", "zset_key", "baz"});
    if (resp != ":0\r\n") {
        return test_failed("ZRANK baz", "Expected :0\\r\\n, got: " + escape_string(resp));
    }
    
    resp = client.send_command({"ZRANK", "zset_key", "caz"});
    if (resp != ":1\r\n") {
        return test_failed("ZRANK caz", "Expected :1\\r\\n");
    }
    
    resp = client.send_command({"ZRANK", "zset_key", "bar"});
    if (resp != ":2\r\n") {
        return test_failed("ZRANK bar", "Expected :2\\r\\n (lexicographic order)");
    }
    
    resp = client.send_command({"ZRANK", "zset_key", "foo"});
    if (resp != ":3\r\n") {
        return test_failed("ZRANK foo", "Expected :3\\r\\n");
    }
    
    // Test missing member
    resp = client.send_command({"ZRANK", "zset_key", "missing"});
    if (resp != "$-1\r\n") {
        return test_failed("ZRANK missing", "Expected $-1\\r\\n (null)");
    }
    
    return test_passed("ZRANK command");
}

// ============= Stage 4: ZRANGE =============

bool test_zrange_command() {
    std::cout << "\n=== Testing ZRANGE command ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZRANGE", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZRANGE", "Could not connect");
    }
    
    // Add members
    client.send_command({"ZADD", "zset_key", "100.0", "foo"});
    client.send_command({"ZADD", "zset_key", "100.0", "bar"});
    client.send_command({"ZADD", "zset_key", "20.0", "baz"});
    client.send_command({"ZADD", "zset_key", "30.1", "caz"});
    client.send_command({"ZADD", "zset_key", "40.2", "paz"});
    
    // Expected order: baz(20.0), caz(30.1), paz(40.2), bar(100.0), foo(100.0)
    
    // Test ZRANGE 0 2 (first 3 elements)
    auto resp = client.send_command({"ZRANGE", "zset_key", "0", "2"});
    
    // Should be array of 3: ["baz", "caz", "paz"]
    if (resp.find("*3\r\n") != 0) {
        return test_failed("ZRANGE 0 2", "Expected array of 3, got: " + escape_string(resp));
    }
    
    if (resp.find("baz") == std::string::npos || 
        resp.find("caz") == std::string::npos || 
        resp.find("paz") == std::string::npos) {
        return test_failed("ZRANGE 0 2", "Missing expected members");
    }
    
    // Test ZRANGE on empty/missing key
    resp = client.send_command({"ZRANGE", "missing_key", "0", "10"});
    if (resp != "*0\r\n") {
        return test_failed("ZRANGE missing", "Expected *0\\r\\n for missing key");
    }
    
    return test_passed("ZRANGE command");
}

// ============= Stage 5: ZRANGE with Negative Indexes =============

bool test_zrange_negative_indexes() {
    std::cout << "\n=== Testing ZRANGE with negative indexes ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZRANGE negative", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZRANGE negative", "Could not connect");
    }
    
    // Add members
    client.send_command({"ZADD", "zset_key", "20.0", "foo"});
    client.send_command({"ZADD", "zset_key", "30.1", "bar"});
    client.send_command({"ZADD", "zset_key", "40.2", "baz"});
    client.send_command({"ZADD", "zset_key", "25.0", "paz"});
    client.send_command({"ZADD", "zset_key", "25.0", "caz"});
    
    // Expected order: foo(20.0), caz(25.0), paz(25.0), bar(30.1), baz(40.2)
    
    // Test ZRANGE 2 -1 (from index 2 to end)
    auto resp = client.send_command({"ZRANGE", "zset_key", "2", "-1"});
    
    // Should get: ["paz", "bar", "baz"]
    if (resp.find("*3\r\n") != 0) {
        return test_failed("ZRANGE 2 -1", "Expected array of 3");
    }
    
    if (resp.find("paz") == std::string::npos || 
        resp.find("bar") == std::string::npos || 
        resp.find("baz") == std::string::npos) {
        return test_failed("ZRANGE 2 -1", "Missing expected members");
    }
    
    // Test ZRANGE -2 -1 (last 2 elements)
    resp = client.send_command({"ZRANGE", "zset_key", "-2", "-1"});
    
    // Should get: ["bar", "baz"]
    if (resp.find("*2\r\n") != 0) {
        return test_failed("ZRANGE -2 -1", "Expected array of 2");
    }
    
    if (resp.find("bar") == std::string::npos || resp.find("baz") == std::string::npos) {
        return test_failed("ZRANGE -2 -1", "Missing expected members");
    }
    
    return test_passed("ZRANGE with negative indexes");
}

// ============= Stage 6: ZCARD =============

bool test_zcard_command() {
    std::cout << "\n=== Testing ZCARD command ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZCARD", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZCARD", "Could not connect");
    }
    
    // Test on missing key
    auto resp = client.send_command({"ZCARD", "missing_key"});
    if (resp != ":0\r\n") {
        return test_failed("ZCARD missing", "Expected :0\\r\\n for missing key");
    }
    
    // Add members
    client.send_command({"ZADD", "zset_key", "20.0", "member1"});
    client.send_command({"ZADD", "zset_key", "30.1", "member2"});
    client.send_command({"ZADD", "zset_key", "40.2", "member3"});
    client.send_command({"ZADD", "zset_key", "50.3", "member4"});
    
    resp = client.send_command({"ZCARD", "zset_key"});
    if (resp != ":4\r\n") {
        return test_failed("ZCARD", "Expected :4\\r\\n, got: " + escape_string(resp));
    }
    
    // Update existing member (should not change cardinality)
    client.send_command({"ZADD", "zset_key", "100.0", "member1"});
    
    resp = client.send_command({"ZCARD", "zset_key"});
    if (resp != ":4\r\n") {
        return test_failed("ZCARD after update", "Expected :4\\r\\n after update");
    }
    
    return test_passed("ZCARD command");
}

// ============= Stage 7: ZSCORE =============

bool test_zscore_command() {
    std::cout << "\n=== Testing ZSCORE command ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZSCORE", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZSCORE", "Could not connect");
    }
    
    // Add members
    client.send_command({"ZADD", "zset_key", "24.34", "one"});
    client.send_command({"ZADD", "zset_key", "90.34", "two"});
    
    // Get score of "one"
    auto resp = client.send_command({"ZSCORE", "zset_key", "one"});
    if (resp != "$5\r\n24.34\r\n") {
        return test_failed("ZSCORE one", "Expected $5\\r\\n24.34\\r\\n, got: " + escape_string(resp));
    }
    
    // Update score
    client.send_command({"ZADD", "zset_key", "100.99", "one"});
    
    resp = client.send_command({"ZSCORE", "zset_key", "one"});
    if (resp != "$6\r\n100.99\r\n") {
        return test_failed("ZSCORE updated", "Expected $6\\r\\n100.99\\r\\n after update");
    }
    
    // Test missing member
    resp = client.send_command({"ZSCORE", "zset_key", "missing"});
    if (resp != "$-1\r\n") {
        return test_failed("ZSCORE missing", "Expected $-1\\r\\n (null)");
    }
    
    return test_passed("ZSCORE command");
}

// ============= Stage 8: ZREM =============

bool test_zrem_command() {
    std::cout << "\n=== Testing ZREM command ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("ZREM", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("ZREM", "Could not connect");
    }
    
    // Create sorted set
    client.send_command({"ZADD", "zset_key", "80.5", "foo"});
    client.send_command({"ZADD", "zset_key", "50.3", "baz"});
    client.send_command({"ZADD", "zset_key", "80.5", "bar"});
    
    // Remove member
    auto resp = client.send_command({"ZREM", "zset_key", "baz"});
    if (resp != ":1\r\n") {
        return test_failed("ZREM", "Expected :1\\r\\n, got: " + escape_string(resp));
    }
    
    // Verify removed with ZRANGE
    resp = client.send_command({"ZRANGE", "zset_key", "0", "-1"});
    
    // Should only have "bar" and "foo" (lexicographic order at same score)
    if (resp.find("*2\r\n") != 0) {
        return test_failed("ZREM verify", "Expected 2 members remaining");
    }
    
    if (resp.find("bar") == std::string::npos || resp.find("foo") == std::string::npos) {
        return test_failed("ZREM verify", "Wrong members remaining");
    }
    
    if (resp.find("baz") != std::string::npos) {
        return test_failed("ZREM verify", "Removed member still present");
    }
    
    // Remove non-existent member
    resp = client.send_command({"ZREM", "zset_key", "missing"});
    if (resp != ":0\r\n") {
        return test_failed("ZREM missing", "Expected :0\\r\\n for missing member");
    }
    
    return test_passed("ZREM command");
}

int main() {
    std::cout << "=====================================" << std::endl;
    std::cout << "    Redis Sorted Sets Test Suite" << std::endl;
    std::cout << "=====================================" << std::endl;
    
    int passed = 0;
    int total = 0;
    
    std::vector<std::pair<std::string, bool(*)()>> tests = {
        {"ZADD create sorted set", test_zadd_create_sorted_set},
        {"ZADD add members", test_zadd_add_members},
        {"ZRANK command", test_zrank_command},
        {"ZRANGE command", test_zrange_command},
        {"ZRANGE with negative indexes", test_zrange_negative_indexes},
        {"ZCARD command", test_zcard_command},
        {"ZSCORE command", test_zscore_command},
        {"ZREM command", test_zrem_command}
    };
    
    for (const auto& [name, test_func] : tests) {
        total++;
        try {
            if (test_func()) {
                passed++;
            }
        } catch (const std::exception& e) {
            test_failed(name, std::string("Exception: ") + e.what());
        }
    }
    
    std::cout << "\n=====================================" << std::endl;
    std::cout << "Results: " << passed << "/" << total << " tests passed" << std::endl;
    
    if (passed == total) {
        std::cout << "✓ All sorted sets tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "✗ Some tests failed" << std::endl;
        return 1;
    }
}