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

// Helper to display responses for debugging
std::string escape_string(const std::string& s) {
    std::stringstream ss;
    for (char c : s) {
        if (c == '\r') ss << "\\r";
        else if (c == '\n') ss << "\\n";
        else ss << c;
    }
    return ss.str();
}

// ============= INCR Command Tests =============

bool test_incr_existing_numeric() {
    std::cout << "\n=== Testing INCR with existing numeric key ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("INCR existing numeric", "Could not connect to server");
    }
    
    // Set initial value
    auto resp = client.send_command({"SET", "counter", "41"});
    if (resp != "+OK\r\n") {
        return test_failed("INCR setup", "Failed to SET initial value");
    }
    
    // INCR should increment to 42
    resp = client.send_command({"INCR", "counter"});
    if (resp != ":42\r\n") {
        return test_failed("INCR existing", "Expected :42\\r\\n, got: " + escape_string(resp));
    }
    
    // INCR again should give 43
    resp = client.send_command({"INCR", "counter"});
    if (resp != ":43\r\n") {
        return test_failed("INCR twice", "Expected :43\\r\\n, got: " + escape_string(resp));
    }
    
    // Test with negative number
    client.send_command({"SET", "negative", "-10"});
    resp = client.send_command({"INCR", "negative"});
    if (resp != ":-9\r\n") {
        return test_failed("INCR negative", "Expected :-9\\r\\n, got: " + escape_string(resp));
    }
    
    return test_passed("INCR existing numeric");
}

bool test_incr_nonexistent() {
    std::cout << "\n=== Testing INCR with non-existent key ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("INCR non-existent", "Could not connect to server");
    }
    
    // INCR on non-existent key should return 1
    auto resp = client.send_command({"INCR", "new_counter"});
    if (resp != ":1\r\n") {
        return test_failed("INCR non-existent", "Expected :1\\r\\n, got: " + escape_string(resp));
    }
    
    // Verify it was set
    resp = client.send_command({"GET", "new_counter"});
    if (resp != "$1\r\n1\r\n") {
        return test_failed("INCR verify", "Key not properly set to 1");
    }
    
    // INCR multiple non-existent keys
    resp = client.send_command({"INCR", "another_counter"});
    if (resp != ":1\r\n") {
        return test_failed("INCR another non-existent", "Expected :1\\r\\n");
    }
    
    return test_passed("INCR non-existent");
}

bool test_incr_non_numeric() {
    std::cout << "\n=== Testing INCR with non-numeric value ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("INCR non-numeric", "Could not connect to server");
    }
    
    // Set non-numeric value
    client.send_command({"SET", "text_key", "xyz"});
    
    // INCR should return error
    auto resp = client.send_command({"INCR", "text_key"});
    if (resp != "-ERR value is not an integer or out of range\r\n") {
        return test_failed("INCR non-numeric", "Expected error, got: " + escape_string(resp));
    }
    
    // Try with partially numeric
    client.send_command({"SET", "partial", "123abc"});
    resp = client.send_command({"INCR", "partial"});
    if (resp != "-ERR value is not an integer or out of range\r\n") {
        return test_failed("INCR partial numeric", "Expected error for partial numeric");
    }
    
    // Try with empty string
    client.send_command({"SET", "empty", ""});
    resp = client.send_command({"INCR", "empty"});
    if (resp != "-ERR value is not an integer or out of range\r\n") {
        return test_failed("INCR empty string", "Expected error for empty string");
    }
    
    return test_passed("INCR non-numeric");
}

// ============= MULTI/EXEC Basic Tests =============

bool test_multi_command() {
    std::cout << "\n=== Testing MULTI command ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("MULTI", "Could not connect to server");
    }
    
    // MULTI should return OK
    auto resp = client.send_command({"MULTI"});
    if (resp != "+OK\r\n") {
        return test_failed("MULTI", "Expected +OK\\r\\n, got: " + escape_string(resp));
    }
    
    // Can call MULTI multiple times (resets transaction)
    resp = client.send_command({"MULTI"});
    if (resp != "+OK\r\n") {
        return test_failed("MULTI twice", "Second MULTI should also return OK");
    }
    
    return test_passed("MULTI");
}

bool test_exec_without_multi() {
    std::cout << "\n=== Testing EXEC without MULTI ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("EXEC without MULTI", "Could not connect to server");
    }
    
    // EXEC without MULTI should return error
    auto resp = client.send_command({"EXEC"});
    if (resp != "-ERR EXEC without MULTI\r\n") {
        return test_failed("EXEC without MULTI", "Expected error, got: " + escape_string(resp));
    }
    
    return test_passed("EXEC without MULTI");
}

bool test_empty_transaction() {
    std::cout << "\n=== Testing empty transaction ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Empty transaction", "Could not connect to server");
    }
    
    // Start transaction
    client.send_command({"MULTI"});
    
    // Execute immediately (empty transaction)
    auto resp = client.send_command({"EXEC"});
    if (resp != "*0\r\n") {
        return test_failed("Empty EXEC", "Expected *0\\r\\n, got: " + escape_string(resp));
    }
    
    // EXEC again should fail (no longer in transaction)
    resp = client.send_command({"EXEC"});
    if (resp != "-ERR EXEC without MULTI\r\n") {
        return test_failed("EXEC after EXEC", "Should not be in transaction after EXEC");
    }
    
    return test_passed("Empty transaction");
}

// ============= Command Queuing Tests =============

bool test_command_queuing() {
    std::cout << "\n=== Testing command queuing ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Command queuing", "Could not connect to server");
    }
    
    // Start transaction
    client.send_command({"MULTI"});
    
    // Queue SET command
    auto resp = client.send_command({"SET", "queue_test", "41"});
    if (resp != "+QUEUED\r\n") {
        return test_failed("Queue SET", "Expected +QUEUED\\r\\n, got: " + escape_string(resp));
    }
    
    // Queue INCR command
    resp = client.send_command({"INCR", "queue_test"});
    if (resp != "+QUEUED\r\n") {
        return test_failed("Queue INCR", "Expected +QUEUED\\r\\n, got: " + escape_string(resp));
    }
    
    // Queue GET command
    resp = client.send_command({"GET", "queue_test"});
    if (resp != "+QUEUED\r\n") {
        return test_failed("Queue GET", "Expected +QUEUED\\r\\n, got: " + escape_string(resp));
    }
    
    // Verify key doesn't exist yet (commands are queued, not executed)
    RedisClient verifier;
    verifier.connect();
    resp = verifier.send_command({"GET", "queue_test"});
    if (resp != "$-1\r\n") {
        return test_failed("Queued not executed", "Commands should not be executed until EXEC");
    }
    
    return test_passed("Command queuing");
}

bool test_transaction_execution() {
    std::cout << "\n=== Testing transaction execution ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Transaction execution", "Could not connect to server");
    }
    
    // Start transaction and queue commands
    client.send_command({"MULTI"});
    client.send_command({"SET", "exec_test", "6"});
    client.send_command({"INCR", "exec_test"});
    client.send_command({"INCR", "new_key"});
    client.send_command({"GET", "new_key"});
    
    // Execute transaction
    auto resp = client.send_command({"EXEC"});
    
    // Expected: array of 4 responses
    // *4\r\n+OK\r\n:7\r\n:1\r\n$1\r\n1\r\n
    if (resp.find("*4\r\n") != 0) {
        return test_failed("EXEC array", "Expected array of 4 responses, got: " + escape_string(resp));
    }
    
    // Verify responses are included
    if (resp.find("+OK\r\n") == std::string::npos) {
        return test_failed("EXEC SET response", "Missing OK response from SET");
    }
    if (resp.find(":7\r\n") == std::string::npos) {
        return test_failed("EXEC INCR response", "Missing :7 from first INCR");
    }
    if (resp.find(":1\r\n") == std::string::npos) {
        return test_failed("EXEC INCR new", "Missing :1 from second INCR");
    }
    
    // Verify changes persisted
    resp = client.send_command({"GET", "exec_test"});
    if (resp != "$1\r\n7\r\n") {
        return test_failed("EXEC persistence", "Value not persisted after EXEC");
    }
    
    return test_passed("Transaction execution");
}

// ============= DISCARD Tests =============

bool test_discard_command() {
    std::cout << "\n=== Testing DISCARD command ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("DISCARD", "Could not connect to server");
    }
    
    // Start transaction and queue commands
    client.send_command({"MULTI"});
    client.send_command({"SET", "discard_test", "41"});
    client.send_command({"INCR", "discard_test"});
    
    // Discard transaction
    auto resp = client.send_command({"DISCARD"});
    if (resp != "+OK\r\n") {
        return test_failed("DISCARD", "Expected +OK\\r\\n, got: " + escape_string(resp));
    }
    
    // Verify commands were not executed
    resp = client.send_command({"GET", "discard_test"});
    if (resp != "$-1\r\n") {
        return test_failed("DISCARD effect", "Commands should not execute after DISCARD");
    }
    
    // DISCARD without transaction should error
    resp = client.send_command({"DISCARD"});
    if (resp != "-ERR DISCARD without MULTI\r\n") {
        return test_failed("DISCARD without MULTI", "Expected error, got: " + escape_string(resp));
    }
    
    return test_passed("DISCARD");
}

// ============= Error Handling in Transactions =============

bool test_transaction_errors() {
    std::cout << "\n=== Testing errors within transactions ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Transaction errors", "Could not connect to server");
    }
    
    // Setup: create keys with different types
    client.send_command({"SET", "string_key", "abc"});
    client.send_command({"SET", "number_key", "41"});
    
    // Start transaction with mixed success/failure
    client.send_command({"MULTI"});
    client.send_command({"INCR", "string_key"});  // Will fail
    client.send_command({"INCR", "number_key"});  // Will succeed
    client.send_command({"SET", "new_key", "value"});  // Will succeed
    
    // Execute and check response
    auto resp = client.send_command({"EXEC"});
    
    // Should have array of 3 responses
    if (resp.find("*3\r\n") != 0) {
        return test_failed("Error array", "Expected array of 3 responses");
    }
    
    // First should be error
    if (resp.find("-ERR value is not an integer or out of range\r\n") == std::string::npos) {
        return test_failed("Error in transaction", "Missing error for INCR on string");
    }
    
    // Second should be success (42)
    if (resp.find(":42\r\n") == std::string::npos) {
        return test_failed("Success after error", "INCR should succeed despite previous error");
    }
    
    // Third should be OK
    if (resp.find("+OK\r\n") == std::string::npos) {
        return test_failed("SET after errors", "SET should succeed");
    }
    
    // Verify successful commands were executed
    resp = client.send_command({"GET", "number_key"});
    if (resp != "$2\r\n42\r\n") {
        return test_failed("Error transaction persistence", "Successful commands should persist");
    }
    
    return test_passed("Transaction errors");
}

// ============= Multiple Concurrent Transactions =============

bool test_multiple_transactions() {
    std::cout << "\n=== Testing multiple concurrent transactions ===" << std::endl;
    
    RedisClient client1, client2;
    if (!client1.connect() || !client2.connect()) {
        return test_failed("Multiple transactions", "Could not connect clients");
    }
    
    // Start transaction on client1
    client1.send_command({"MULTI"});
    client1.send_command({"SET", "shared_key", "41"});
    client1.send_command({"INCR", "shared_key"});
    
    // Start transaction on client2
    client2.send_command({"MULTI"});
    client2.send_command({"INCR", "shared_key"});
    
    // Execute client1's transaction first
    auto resp1 = client1.send_command({"EXEC"});
    if (resp1.find("*2\r\n") != 0) {
        return test_failed("Client1 EXEC", "Expected array of 2 responses");
    }
    if (resp1.find(":42\r\n") == std::string::npos) {
        return test_failed("Client1 INCR", "Expected 42 from INCR");
    }
    
    // Execute client2's transaction
    auto resp2 = client2.send_command({"EXEC"});
    if (resp2.find("*1\r\n") != 0) {
        return test_failed("Client2 EXEC", "Expected array of 1 response");
    }
    if (resp2.find(":43\r\n") == std::string::npos) {
        return test_failed("Client2 INCR", "Expected 43 (incremented from 42)");
    }
    
    return test_passed("Multiple transactions");
}

bool test_transaction_isolation() {
    std::cout << "\n=== Testing transaction isolation ===" << std::endl;
    
    RedisClient client1, client2;
    if (!client1.connect() || !client2.connect()) {
        return test_failed("Transaction isolation", "Could not connect clients");
    }
    
    // Client1 starts transaction
    client1.send_command({"MULTI"});
    client1.send_command({"SET", "iso_test", "100"});
    
    // Client2 performs operations outside transaction
    auto resp = client2.send_command({"GET", "iso_test"});
    if (resp != "$-1\r\n") {
        return test_failed("Isolation before EXEC", "Client1's queued commands visible to Client2");
    }
    
    // Client2 sets the same key
    client2.send_command({"SET", "iso_test", "200"});
    
    // Client1 continues transaction
    client1.send_command({"INCR", "iso_test"});
    
    // Execute Client1's transaction
    resp = client1.send_command({"EXEC"});
    
    // Client1's SET and INCR should override Client2's value
    resp = client2.send_command({"GET", "iso_test"});
    if (resp != "$3\r\n101\r\n") {
        return test_failed("Isolation after EXEC", "Transaction didn't properly override");
    }
    
    return test_passed("Transaction isolation");
}

// ============= Complex Transaction Scenarios =============

bool test_nested_multi() {
    std::cout << "\n=== Testing nested MULTI (should reset) ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Nested MULTI", "Could not connect to server");
    }
    
    // Start transaction
    client.send_command({"MULTI"});
    client.send_command({"SET", "nested", "1"});
    
    // Call MULTI again (should reset transaction)
    auto resp = client.send_command({"MULTI"});
    if (resp != "+OK\r\n") {
        return test_failed("Second MULTI", "MULTI should return OK");
    }
    
    // Queue new command
    client.send_command({"SET", "nested", "2"});
    
    // Execute should only have the second SET
    resp = client.send_command({"EXEC"});
    if (resp != "*1\r\n+OK\r\n") {
        return test_failed("Nested EXEC", "Should only execute commands after second MULTI");
    }
    
    // Verify value
    resp = client.send_command({"GET", "nested"});
    if (resp != "$1\r\n2\r\n") {
        return test_failed("Nested result", "Should have value from second transaction");
    }
    
    return test_passed("Nested MULTI");
}

bool test_transaction_with_lists() {
    std::cout << "\n=== Testing transactions with list commands ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Transaction lists", "Could not connect to server");
    }
    
    // Transaction with list operations
    client.send_command({"MULTI"});
    client.send_command({"RPUSH", "trans_list", "a", "b", "c"});
    client.send_command({"LPOP", "trans_list"});
    client.send_command({"LLEN", "trans_list"});
    client.send_command({"LRANGE", "trans_list", "0", "-1"});
    
    auto resp = client.send_command({"EXEC"});
    
    // Verify array of 4 responses
    if (resp.find("*4\r\n") != 0) {
        return test_failed("List transaction array", "Expected array of 4");
    }
    
    // Check individual responses
    if (resp.find(":3\r\n") == std::string::npos) {  // RPUSH returns size
        return test_failed("List RPUSH response", "Missing :3 from RPUSH");
    }
    if (resp.find("$1\r\na\r\n") == std::string::npos) {  // LPOP returns "a"
        return test_failed("List LPOP response", "Missing 'a' from LPOP");
    }
    if (resp.find(":2\r\n") == std::string::npos) {  // LLEN returns 2
        return test_failed("List LLEN response", "Missing :2 from LLEN");
    }
    
    return test_passed("Transaction with lists");
}

bool test_long_transaction() {
    std::cout << "\n=== Testing long transaction with many commands ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Long transaction", "Could not connect to server");
    }
    
    // Start transaction with many commands
    client.send_command({"MULTI"});
    
    // Queue 20 commands
    for (int i = 0; i < 20; i++) {
        std::string key = "long_key_" + std::to_string(i);
        std::string value = std::to_string(i * 10);
        auto resp = client.send_command({"SET", key, value});
        if (resp != "+QUEUED\r\n") {
            return test_failed("Long queue", "Failed to queue command " + std::to_string(i));
        }
    }
    
    // Execute
    auto resp = client.send_command({"EXEC"});
    
    // Should have array of 20 OK responses
    if (resp.find("*20\r\n") != 0) {
        return test_failed("Long EXEC array", "Expected array of 20");
    }
    
    // Count OK responses
    size_t pos = 0;
    int ok_count = 0;
    while ((pos = resp.find("+OK\r\n", pos)) != std::string::npos) {
        ok_count++;
        pos += 5;
    }
    
    if (ok_count != 20) {
        return test_failed("Long EXEC OKs", "Expected 20 OK responses, got " + std::to_string(ok_count));
    }
    
    // Verify some values
    resp = client.send_command({"GET", "long_key_10"});
    if (resp != "$3\r\n100\r\n") {
        return test_failed("Long transaction verify", "Values not properly set");
    }
    
    return test_passed("Long transaction");
}

bool test_transaction_after_discard() {
    std::cout << "\n=== Testing new transaction after DISCARD ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("After DISCARD", "Could not connect to server");
    }
    
    // First transaction - will be discarded
    client.send_command({"MULTI"});
    client.send_command({"SET", "discard_key", "wrong"});
    client.send_command({"DISCARD"});
    
    // New transaction
    client.send_command({"MULTI"});
    client.send_command({"SET", "discard_key", "correct"});
    auto resp = client.send_command({"EXEC"});
    
    if (resp != "*1\r\n+OK\r\n") {
        return test_failed("After DISCARD EXEC", "New transaction should work after DISCARD");
    }
    
    // Verify correct value
    resp = client.send_command({"GET", "discard_key"});
    if (resp != "$7\r\ncorrect\r\n") {
        return test_failed("After DISCARD value", "Should have value from second transaction");
    }
    
    return test_passed("Transaction after DISCARD");
}

// ============= Edge Cases =============

bool test_incr_overflow() {
    std::cout << "\n=== Testing INCR with large numbers ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("INCR overflow", "Could not connect to server");
    }
    
    // Test with max int
    client.send_command({"SET", "big_num", "2147483646"});
    auto resp = client.send_command({"INCR", "big_num"});
    if (resp != ":2147483647\r\n") {
        return test_failed("INCR large", "Failed with large number");
    }
    
    // One more increment might overflow (implementation dependent)
    resp = client.send_command({"INCR", "big_num"});
    // This could either wrap around or error - just verify we get a response
    if (resp.empty()) {
        return test_failed("INCR overflow", "No response for potential overflow");
    }
    
    return test_passed("INCR overflow");
}

bool test_queued_unknown_command() {
    std::cout << "\n=== Testing unknown command in transaction ===" << std::endl;
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Unknown in transaction", "Could not connect to server");
    }
    
    client.send_command({"MULTI"});
    client.send_command({"SET", "before", "1"});
    auto resp = client.send_command({"UNKNOWN_CMD", "arg"});
    
    // Unknown commands should still be queued
    if (resp != "+QUEUED\r\n") {
        return test_failed("Queue unknown", "Unknown command should be queued");
    }
    
    client.send_command({"SET", "after", "2"});
    
    // Execute - unknown command should error but others succeed
    resp = client.send_command({"EXEC"});
    if (resp.find("*3\r\n") != 0) {
        return test_failed("Unknown EXEC array", "Expected array of 3");
    }
    
    // Should contain OK for SET and error for unknown
    if (resp.find("+OK\r\n") == std::string::npos) {
        return test_failed("Unknown EXEC SET", "SET commands should succeed");
    }
    if (resp.find("-ERR") == std::string::npos) {
        return test_failed("Unknown EXEC error", "Unknown command should error");
    }
    
    return test_passed("Unknown in transaction");
}

int main() {
    std::cout << "=====================================" << std::endl;
    std::cout << "    Redis Transactions Test Suite" << std::endl;
    std::cout << "=====================================" << std::endl;
    
    int passed = 0;
    int total = 0;
    
    std::vector<std::pair<std::string, bool(*)()>> tests = {
        // INCR tests
        {"INCR existing numeric", test_incr_existing_numeric},
        {"INCR non-existent", test_incr_nonexistent},
        {"INCR non-numeric", test_incr_non_numeric},
        
        // Basic transaction tests
        {"MULTI command", test_multi_command},
        {"EXEC without MULTI", test_exec_without_multi},
        {"Empty transaction", test_empty_transaction},
        
        // Command queuing
        {"Command queuing", test_command_queuing},
        {"Transaction execution", test_transaction_execution},
        
        // DISCARD
        {"DISCARD command", test_discard_command},
        
        // Error handling
        {"Transaction errors", test_transaction_errors},
        
        // Multiple clients
        {"Multiple transactions", test_multiple_transactions},
        {"Transaction isolation", test_transaction_isolation},
        
        // Complex scenarios
        {"Nested MULTI", test_nested_multi},
        {"Transaction with lists", test_transaction_with_lists},
        {"Long transaction", test_long_transaction},
        {"Transaction after DISCARD", test_transaction_after_discard},
        
        // Edge cases
        {"INCR overflow", test_incr_overflow},
        {"Unknown command in transaction", test_queued_unknown_command}
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
        std::cout << "All tests passed" << std::endl;
        return 0;
    } else {
        std::cout << "Some tests failed" << std::endl;
        return 1;
    }
}