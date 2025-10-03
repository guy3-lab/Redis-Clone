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
#include <sstream>
#include <fstream>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <sys/stat.h>

class RedisClient {
public:
    int sock;
    
public:
    RedisClient() : sock(-1) {}
    
    ~RedisClient() {
        if (sock >= 0) close(sock);
    }
    
    bool connect(const std::string& host = "127.0.0.1", int port = 6379) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) return false;
        
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr(host.c_str());
        
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
};

class RedisServer {
public:
    pid_t pid;
    int port;
    
    RedisServer(int p = 6379) : pid(-1), port(p) {}
    
    ~RedisServer() {
        stop();
    }
    
    bool start(const std::string& args = "") {
        pid = fork();
        if (pid == 0) {
            // Child process
            std::string cmd = "./Server --port " + std::to_string(port);
            if (!args.empty()) {
                cmd += " " + args;
            }
            
            // Redirect output to reduce clutter
            freopen("/dev/null", "w", stdout);
            freopen("/dev/null", "w", stderr);
            
            execl("/bin/sh", "sh", "-c", cmd.c_str(), nullptr);
            exit(1);
        } else if (pid > 0) {
            // Parent process - wait for server to start
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

// Test helper functions
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
        else if (c < 32 || c > 126) ss << "\\x" << std::hex << (int)(unsigned char)c;
        else ss << c;
    }
    return ss.str();
}

// Helper to create test directory and RDB file
bool create_test_rdb(const std::string& dir, const std::string& filename, const std::vector<unsigned char>& data) {
    // Create directory if it doesn't exist
    mkdir(dir.c_str(), 0755);
    
    std::string filepath = dir + "/" + filename;
    std::ofstream file(filepath, std::ios::binary);
    if (!file) {
        return false;
    }
    
    file.write(reinterpret_cast<const char*>(data.data()), data.size());
    file.close();
    return true;
}

void cleanup_test_rdb(const std::string& dir, const std::string& filename) {
    std::string filepath = dir + "/" + filename;
    unlink(filepath.c_str());
    rmdir(dir.c_str());
}

// Empty RDB file (valid but contains no keys)
std::vector<unsigned char> get_empty_rdb() {
    return {
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,  // REDIS0011
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa,
        0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69,
        0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69,
        0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08,
        0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2,
        0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66,
        0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0,
        0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
    };
}

// RDB with single key "foo" = "bar"
std::vector<unsigned char> get_single_key_rdb() {
    return {
        // Header
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,  // REDIS0011
        // Metadata
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30,
        // Database section
        0xfe, 0x00,  // DB 0
        0xfb, 0x01, 0x00,  // 1 key, 0 with expiry
        0x00,  // String type
        0x03, 0x66, 0x6f, 0x6f,  // Key: "foo"
        0x03, 0x62, 0x61, 0x72,  // Value: "bar"
        // End
        0xff,
        0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
    };
}

// RDB with multiple keys
std::vector<unsigned char> get_multiple_keys_rdb() {
    return {
        // Header
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
        // Metadata
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30,
        // Database section
        0xfe, 0x00,  // DB 0
        0xfb, 0x03, 0x00,  // 3 keys, 0 with expiry
        // Key 1: foo = bar
        0x00, 0x03, 0x66, 0x6f, 0x6f, 0x03, 0x62, 0x61, 0x72,
        // Key 2: baz = qux
        0x00, 0x03, 0x62, 0x61, 0x7a, 0x03, 0x71, 0x75, 0x78,
        // Key 3: hello = world
        0x00, 0x05, 0x68, 0x65, 0x6c, 0x6c, 0x6f,
        0x05, 0x77, 0x6f, 0x72, 0x6c, 0x64,
        // End
        0xff,
        0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
    };
}

// RDB with key that has expiry in the future (won't expire)
std::vector<unsigned char> get_non_expired_key_rdb() {
    // Expiry timestamp far in the future: 9999999999000 ms (year 2286)
    return {
        // Header
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
        // Metadata
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30,
        // Database section
        0xfe, 0x00,  // DB 0
        0xfb, 0x01, 0x01,  // 1 key, 1 with expiry
        0xfc,  // Expiry in milliseconds
        0x38, 0x5f, 0x8a, 0x43, 0x19, 0x09, 0x00, 0x00,  // 9999999999000 ms (little-endian)
        0x00,  // String type
        0x03, 0x66, 0x6f, 0x6f,  // Key: "foo"
        0x03, 0x62, 0x61, 0x72,  // Value: "bar"
        // End
        0xff,
        0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
    };
}

// RDB with key that has already expired
std::vector<unsigned char> get_expired_key_rdb() {
    // Expiry timestamp in the past: 1000000000000 ms (year 2001)
    return {
        // Header
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
        // Metadata
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30,
        // Database section
        0xfe, 0x00,  // DB 0
        0xfb, 0x01, 0x01,  // 1 key, 1 with expiry
        0xfc,  // Expiry in milliseconds
        0x00, 0x40, 0x7a, 0x10, 0xf4, 0x00, 0x00, 0x00,  // 1000000000000 ms (little-endian)
        0x00,  // String type
        0x03, 0x66, 0x6f, 0x6f,  // Key: "foo"
        0x03, 0x62, 0x61, 0x72,  // Value: "bar"
        // End
        0xff,
        0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
    };
}

// RDB with mixed expiry states
std::vector<unsigned char> get_mixed_expiry_rdb() {
    return {
        // Header
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
        // Metadata
        0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
        0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30,
        // Database section
        0xfe, 0x00,  // DB 0
        0xfb, 0x03, 0x02,  // 3 keys, 2 with expiry
        // Key 1: no expiry - foo = bar
        0x00, 0x03, 0x66, 0x6f, 0x6f, 0x03, 0x62, 0x61, 0x72,
        // Key 2: expired - old = data
        0xfc,
        0x00, 0x40, 0x7a, 0x10, 0xf4, 0x00, 0x00, 0x00,  // Past
        0x00, 0x03, 0x6f, 0x6c, 0x64, 0x04, 0x64, 0x61, 0x74, 0x61,
        // Key 3: not expired - future = value
        0xfc,
        0x38, 0x5f, 0x8a, 0x43, 0x19, 0x09, 0x00, 0x00,  // Future
        0x00, 0x06, 0x66, 0x75, 0x74, 0x75, 0x72, 0x65,
        0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
        // End
        0xff,
        0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
    };
}

// ============= Stage 1: CONFIG GET (dir and dbfilename) =============

bool test_config_get_dir() {
    std::cout << "\n=== Testing CONFIG GET dir ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start("--dir /tmp/redis-test --dbfilename test.rdb")) {
        return test_failed("CONFIG GET dir", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("CONFIG GET dir", "Could not connect to server");
    }
    
    auto resp = client.send_command({"CONFIG", "GET", "dir"});
    
    // Expected: *2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-test\r\n
    if (resp.find("*2\r\n") != 0) {
        return test_failed("CONFIG GET dir", "Expected array of 2 elements, got: " + escape_string(resp));
    }
    
    if (resp.find("$3\r\ndir\r\n") == std::string::npos) {
        return test_failed("CONFIG GET dir", "Missing 'dir' parameter name");
    }
    
    if (resp.find("/tmp/redis-test") == std::string::npos) {
        return test_failed("CONFIG GET dir", "Missing expected directory path");
    }
    
    return test_passed("CONFIG GET dir");
}

bool test_config_get_dbfilename() {
    std::cout << "\n=== Testing CONFIG GET dbfilename ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start("--dir /tmp/redis-test --dbfilename dump.rdb")) {
        return test_failed("CONFIG GET dbfilename", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("CONFIG GET dbfilename", "Could not connect to server");
    }
    
    auto resp = client.send_command({"CONFIG", "GET", "dbfilename"});
    
    // Expected: *2\r\n$10\r\ndbfilename\r\n$8\r\ndump.rdb\r\n
    if (resp.find("*2\r\n") != 0) {
        return test_failed("CONFIG GET dbfilename", "Expected array of 2 elements");
    }
    
    if (resp.find("dbfilename") == std::string::npos) {
        return test_failed("CONFIG GET dbfilename", "Missing 'dbfilename' parameter name");
    }
    
    if (resp.find("dump.rdb") == std::string::npos) {
        return test_failed("CONFIG GET dbfilename", "Missing expected filename");
    }
    
    return test_passed("CONFIG GET dbfilename");
}

bool test_config_get_custom_values() {
    std::cout << "\n=== Testing CONFIG GET with custom values ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start("--dir /var/data/redis --dbfilename production.rdb")) {
        return test_failed("CONFIG GET custom", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("CONFIG GET custom", "Could not connect to server");
    }
    
    // Test dir
    auto resp = client.send_command({"CONFIG", "GET", "dir"});
    if (resp.find("/var/data/redis") == std::string::npos) {
        return test_failed("CONFIG GET custom dir", "Incorrect dir value");
    }
    
    // Test dbfilename
    resp = client.send_command({"CONFIG", "GET", "dbfilename"});
    if (resp.find("production.rdb") == std::string::npos) {
        return test_failed("CONFIG GET custom dbfilename", "Incorrect dbfilename value");
    }
    
    return test_passed("CONFIG GET custom values");
}

// ============= Stage 2: KEYS command with single key =============

bool test_keys_with_no_rdb() {
    std::cout << "\n=== Testing KEYS * with no RDB file ===" << std::endl;
    
    std::string dir = "/tmp/test_no_rdb";
    mkdir(dir.c_str(), 0755);
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename nonexistent.rdb")) {
        rmdir(dir.c_str());
        return test_failed("KEYS no RDB", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        rmdir(dir.c_str());
        return test_failed("KEYS no RDB", "Could not connect to server");
    }
    
    auto resp = client.send_command({"KEYS", "*"});
    
    // Should return empty array
    if (resp != "*0\r\n") {
        rmdir(dir.c_str());
        return test_failed("KEYS no RDB", "Expected empty array, got: " + escape_string(resp));
    }
    
    rmdir(dir.c_str());
    return test_passed("KEYS with no RDB file");
}

bool test_keys_with_empty_rdb() {
    std::cout << "\n=== Testing KEYS * with empty RDB ===" << std::endl;
    
    std::string dir = "/tmp/test_empty_rdb";
    std::string filename = "empty.rdb";
    
    if (!create_test_rdb(dir, filename, get_empty_rdb())) {
        return test_failed("KEYS empty RDB", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS empty RDB", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS empty RDB", "Could not connect to server");
    }
    
    auto resp = client.send_command({"KEYS", "*"});
    
    if (resp != "*0\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS empty RDB", "Expected empty array for empty RDB");
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("KEYS with empty RDB");
}

bool test_keys_with_single_key() {
    std::cout << "\n=== Testing KEYS * with single key ===" << std::endl;
    
    std::string dir = "/tmp/test_single_key";
    std::string filename = "single.rdb";
    
    if (!create_test_rdb(dir, filename, get_single_key_rdb())) {
        return test_failed("KEYS single", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS single", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS single", "Could not connect to server");
    }
    
    auto resp = client.send_command({"KEYS", "*"});
    
    // Should return array with 1 element: *1\r\n$3\r\nfoo\r\n
    if (resp.find("*1\r\n") != 0) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS single", "Expected array of 1 element, got: " + escape_string(resp));
    }
    
    if (resp.find("foo") == std::string::npos) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS single", "Missing key 'foo'");
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("KEYS with single key");
}

// ============= Stage 3: GET command with single key =============

bool test_get_from_rdb() {
    std::cout << "\n=== Testing GET with value from RDB ===" << std::endl;
    
    std::string dir = "/tmp/test_get_rdb";
    std::string filename = "get.rdb";
    
    if (!create_test_rdb(dir, filename, get_single_key_rdb())) {
        return test_failed("GET from RDB", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET from RDB", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET from RDB", "Could not connect to server");
    }
    
    auto resp = client.send_command({"GET", "foo"});
    
    // Expected: $3\r\nbar\r\n
    if (resp != "$3\r\nbar\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET from RDB", "Expected 'bar', got: " + escape_string(resp));
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("GET from RDB");
}

bool test_get_nonexistent_from_rdb() {
    std::cout << "\n=== Testing GET nonexistent key from RDB ===" << std::endl;
    
    std::string dir = "/tmp/test_get_none";
    std::string filename = "none.rdb";
    
    if (!create_test_rdb(dir, filename, get_single_key_rdb())) {
        return test_failed("GET nonexistent", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET nonexistent", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET nonexistent", "Could not connect to server");
    }
    
    auto resp = client.send_command({"GET", "nonexistent"});
    
    // Should return null
    if (resp != "$-1\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET nonexistent", "Expected null, got: " + escape_string(resp));
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("GET nonexistent from RDB");
}

// ============= Stage 4: KEYS with multiple keys =============

bool test_keys_multiple() {
    std::cout << "\n=== Testing KEYS * with multiple keys ===" << std::endl;
    
    std::string dir = "/tmp/test_keys_multi";
    std::string filename = "multi.rdb";
    
    if (!create_test_rdb(dir, filename, get_multiple_keys_rdb())) {
        return test_failed("KEYS multiple", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS multiple", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS multiple", "Could not connect to server");
    }
    
    auto resp = client.send_command({"KEYS", "*"});
    
    // Should return array with 3 elements
    if (resp.find("*3\r\n") != 0) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS multiple", "Expected array of 3 elements, got: " + escape_string(resp));
    }
    
    // Check all keys are present (order may vary)
    if (resp.find("foo") == std::string::npos) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS multiple", "Missing key 'foo'");
    }
    
    if (resp.find("baz") == std::string::npos) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS multiple", "Missing key 'baz'");
    }
    
    if (resp.find("hello") == std::string::npos) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS multiple", "Missing key 'hello'");
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("KEYS with multiple keys");
}

// ============= Stage 5: GET with multiple keys =============

bool test_get_multiple_values() {
    std::cout << "\n=== Testing GET for multiple values ===" << std::endl;
    
    std::string dir = "/tmp/test_get_multi";
    std::string filename = "multi.rdb";
    
    if (!create_test_rdb(dir, filename, get_multiple_keys_rdb())) {
        return test_failed("GET multiple", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET multiple", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET multiple", "Could not connect to server");
    }
    
    // Test foo = bar
    auto resp = client.send_command({"GET", "foo"});
    if (resp != "$3\r\nbar\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET foo", "Expected 'bar', got: " + escape_string(resp));
    }
    
    // Test baz = qux
    resp = client.send_command({"GET", "baz"});
    if (resp != "$3\r\nqux\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET baz", "Expected 'qux', got: " + escape_string(resp));
    }
    
    // Test hello = world
    resp = client.send_command({"GET", "hello"});
    if (resp != "$5\r\nworld\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET hello", "Expected 'world', got: " + escape_string(resp));
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("GET multiple values");
}

// ============= Stage 6: Keys with expiry (non-expired) =============

bool test_get_non_expired_key() {
    std::cout << "\n=== Testing GET with non-expired key ===" << std::endl;
    
    std::string dir = "/tmp/test_non_expired";
    std::string filename = "non_expired.rdb";
    
    if (!create_test_rdb(dir, filename, get_non_expired_key_rdb())) {
        return test_failed("GET non-expired", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET non-expired", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET non-expired", "Could not connect to server");
    }
    
    auto resp = client.send_command({"GET", "foo"});
    
    // Should return the value (not expired)
    if (resp != "$3\r\nbar\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET non-expired", "Expected 'bar', got: " + escape_string(resp));
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("GET non-expired key");
}

bool test_get_expired_key() {
    std::cout << "\n=== Testing GET with expired key ===" << std::endl;
    
    std::string dir = "/tmp/test_expired";
    std::string filename = "expired.rdb";
    
    if (!create_test_rdb(dir, filename, get_expired_key_rdb())) {
        return test_failed("GET expired", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET expired", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET expired", "Could not connect to server");
    }
    
    auto resp = client.send_command({"GET", "foo"});
    
    // Should return null (expired)
    if (resp != "$-1\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("GET expired", "Expected null for expired key, got: " + escape_string(resp));
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("GET expired key");
}

bool test_keys_with_expired() {
    std::cout << "\n=== Testing KEYS * with expired keys ===" << std::endl;
    
    std::string dir = "/tmp/test_keys_expired";
    std::string filename = "keys_expired.rdb";
    
    if (!create_test_rdb(dir, filename, get_expired_key_rdb())) {
        return test_failed("KEYS expired", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS expired", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS expired", "Could not connect to server");
    }
    
    auto resp = client.send_command({"KEYS", "*"});
    
    // Should not include expired key
    if (resp != "*0\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("KEYS expired", "Expired key should not appear in KEYS, got: " + escape_string(resp));
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("KEYS with expired keys");
}

bool test_mixed_expiry_states() {
    std::cout << "\n=== Testing mixed expiry states ===" << std::endl;
    
    std::string dir = "/tmp/test_mixed_expiry";
    std::string filename = "mixed.rdb";
    
    if (!create_test_rdb(dir, filename, get_mixed_expiry_rdb())) {
        return test_failed("Mixed expiry", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("Mixed expiry", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("Mixed expiry", "Could not connect to server");
    }
    
    // Test non-expired keys are returned
    auto resp = client.send_command({"GET", "foo"});
    if (resp != "$3\r\nbar\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("Mixed GET foo", "Expected 'bar'");
    }
    
    resp = client.send_command({"GET", "future"});
    if (resp != "$5\r\nvalue\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("Mixed GET future", "Expected 'value'");
    }
    
    // Test expired key returns null
    resp = client.send_command({"GET", "old"});
    if (resp != "$-1\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("Mixed GET old", "Expected null for expired key");
    }
    
    // KEYS should only show non-expired
    resp = client.send_command({"KEYS", "*"});
    if (resp.find("*2\r\n") != 0) {
        cleanup_test_rdb(dir, filename);
        return test_failed("Mixed KEYS", "Expected 2 keys (non-expired only)");
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("Mixed expiry states");
}

// ============= Integration Tests =============

bool test_rdb_persistence_with_new_writes() {
    std::cout << "\n=== Testing RDB load with new writes ===" << std::endl;
    
    std::string dir = "/tmp/test_rdb_writes";
    std::string filename = "persist.rdb";
    
    if (!create_test_rdb(dir, filename, get_single_key_rdb())) {
        return test_failed("RDB + writes", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("RDB + writes", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("RDB + writes", "Could not connect to server");
    }
    
    // Verify loaded key
    auto resp = client.send_command({"GET", "foo"});
    if (resp != "$3\r\nbar\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("RDB + writes load", "RDB key not loaded");
    }
    
    // Write new key
    resp = client.send_command({"SET", "new_key", "new_value"});
    if (resp != "+OK\r\n") {
        cleanup_test_rdb(dir, filename);
        return test_failed("RDB + writes SET", "Could not set new key");
    }
    
    // Verify both keys
    resp = client.send_command({"KEYS", "*"});
    if (resp.find("foo") == std::string::npos || resp.find("new_key") == std::string::npos) {
        cleanup_test_rdb(dir, filename);
        return test_failed("RDB + writes KEYS", "Missing keys after write");
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("RDB with new writes");
}

bool test_config_values_persist() {
    std::cout << "\n=== Testing CONFIG values don't change ===" << std::endl;
    
    std::string dir = "/tmp/test_config_persist";
    std::string filename = "config.rdb";
    
    if (!create_test_rdb(dir, filename, get_empty_rdb())) {
        return test_failed("CONFIG persist", "Could not create RDB file");
    }
    
    RedisServer server(6379);
    if (!server.start("--dir " + dir + " --dbfilename " + filename)) {
        cleanup_test_rdb(dir, filename);
        return test_failed("CONFIG persist", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        cleanup_test_rdb(dir, filename);
        return test_failed("CONFIG persist", "Could not connect to server");
    }
    
    // Write some data
    client.send_command({"SET", "test", "value"});
    
    // Config values should still be correct
    auto resp = client.send_command({"CONFIG", "GET", "dir"});
    if (resp.find(dir) == std::string::npos) {
        cleanup_test_rdb(dir, filename);
        return test_failed("CONFIG persist dir", "dir value changed");
    }
    
    resp = client.send_command({"CONFIG", "GET", "dbfilename"});
    if (resp.find(filename) == std::string::npos) {
        cleanup_test_rdb(dir, filename);
        return test_failed("CONFIG persist filename", "filename value changed");
    }
    
    cleanup_test_rdb(dir, filename);
    return test_passed("CONFIG values persist");
}

int main() {
    std::cout << "=====================================" << std::endl;
    std::cout << "    Redis RDB Persistence Test Suite" << std::endl;
    std::cout << "=====================================" << std::endl;
    
    int passed = 0;
    int total = 0;
    
    std::vector<std::pair<std::string, bool(*)()>> tests = {
        // Stage 1: CONFIG GET
        {"CONFIG GET dir", test_config_get_dir},
        {"CONFIG GET dbfilename", test_config_get_dbfilename},
        {"CONFIG GET custom values", test_config_get_custom_values},
        
        // Stage 2: KEYS with single key
        {"KEYS with no RDB", test_keys_with_no_rdb},
        {"KEYS with empty RDB", test_keys_with_empty_rdb},
        {"KEYS with single key", test_keys_with_single_key},
        
        // Stage 3: GET with single key
        {"GET from RDB", test_get_from_rdb},
        {"GET nonexistent from RDB", test_get_nonexistent_from_rdb},
        
        // Stage 4: KEYS with multiple keys
        {"KEYS with multiple keys", test_keys_multiple},
        
        // Stage 5: GET with multiple keys
        {"GET multiple values", test_get_multiple_values},
        
        // Stage 6: Keys with expiry
        {"GET non-expired key", test_get_non_expired_key},
        {"GET expired key", test_get_expired_key},
        {"KEYS with expired keys", test_keys_with_expired},
        {"Mixed expiry states", test_mixed_expiry_states},
        
        // Integration tests
        {"RDB with new writes", test_rdb_persistence_with_new_writes},
        {"CONFIG values persist", test_config_values_persist}
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
        std::cout << "✓ All RDB persistence tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "✗ Some tests failed" << std::endl;
        return 1;
    }
}