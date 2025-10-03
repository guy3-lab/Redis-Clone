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
#include <fcntl.h>

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
    
    // Set socket to non-blocking mode
    void set_nonblocking() {
        if (sock >= 0) {
            int flags = fcntl(sock, F_GETFL, 0);
            fcntl(sock, F_SETFL, flags | O_NONBLOCK);
        }
    }
    
    // Set socket back to blocking mode
    void set_blocking() {
        if (sock >= 0) {
            int flags = fcntl(sock, F_GETFL, 0);
            fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
        }
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

// Helper to count occurrences of a substring
int count_occurrences(const std::string& str, const std::string& substr) {
    int count = 0;
    size_t pos = 0;
    while ((pos = str.find(substr, pos)) != std::string::npos) {
        count++;
        pos += substr.length();
    }
    return count;
}

// ============= Stage 1: SUBSCRIBE Command =============

bool test_subscribe_single_channel() {
    std::cout << "\n=== Testing SUBSCRIBE to single channel ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("SUBSCRIBE single", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("SUBSCRIBE single", "Could not connect to server");
    }
    
    auto resp = client.send_command({"SUBSCRIBE", "channel1"});
    
    // Expected format:
    // *3\r\n$9\r\nsubscribe\r\n$8\r\nchannel1\r\n:1\r\n
    // Array with 3 elements: "subscribe", "channel1", 1 (number of subscriptions)
    
    if (resp.find("*3\r\n") == std::string::npos) {
        return test_failed("SUBSCRIBE single", "Expected array of 3 elements, got: " + escape_string(resp));
    }
    
    if (resp.find("subscribe") == std::string::npos) {
        return test_failed("SUBSCRIBE single", "Missing 'subscribe' keyword");
    }
    
    if (resp.find("channel1") == std::string::npos) {
        return test_failed("SUBSCRIBE single", "Missing channel name");
    }
    
    if (resp.find(":1\r\n") == std::string::npos) {
        return test_failed("SUBSCRIBE single", "Missing subscription count");
    }
    
    return test_passed("SUBSCRIBE single channel");
}

bool test_subscribe_multiple_channels() {
    std::cout << "\n=== Testing SUBSCRIBE to multiple channels ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("SUBSCRIBE multiple", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("SUBSCRIBE multiple", "Could not connect to server");
    }
    
    auto resp = client.send_command({"SUBSCRIBE", "channel1", "channel2", "channel3"});
    
    // Should receive 3 subscribe confirmations, one for each channel
    // Each confirmation is *3\r\n$9\r\nsubscribe\r\n$channelN\r\n:N\r\n
    
    int subscribe_count = count_occurrences(resp, "subscribe");
    if (subscribe_count != 3) {
        return test_failed("SUBSCRIBE multiple", "Expected 3 subscribe confirmations, got: " + std::to_string(subscribe_count));
    }
    
    if (resp.find("channel1") == std::string::npos) {
        return test_failed("SUBSCRIBE multiple", "Missing channel1");
    }
    
    if (resp.find("channel2") == std::string::npos) {
        return test_failed("SUBSCRIBE multiple", "Missing channel2");
    }
    
    if (resp.find("channel3") == std::string::npos) {
        return test_failed("SUBSCRIBE multiple", "Missing channel3");
    }
    
    // Should see counts 1, 2, 3
    if (resp.find(":1\r\n") == std::string::npos) {
        return test_failed("SUBSCRIBE multiple", "Missing count :1");
    }
    
    if (resp.find(":2\r\n") == std::string::npos) {
        return test_failed("SUBSCRIBE multiple", "Missing count :2");
    }
    
    if (resp.find(":3\r\n") == std::string::npos) {
        return test_failed("SUBSCRIBE multiple", "Missing count :3");
    }
    
    return test_passed("SUBSCRIBE multiple channels");
}

bool test_subscribe_same_channel_twice() {
    std::cout << "\n=== Testing SUBSCRIBE to same channel twice ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("SUBSCRIBE duplicate", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("SUBSCRIBE duplicate", "Could not connect to server");
    }
    
    // First subscription
    auto resp1 = client.send_command({"SUBSCRIBE", "channel1"});
    if (resp1.find(":1\r\n") == std::string::npos) {
        return test_failed("SUBSCRIBE duplicate", "First subscription should show count 1");
    }
    
    // Second subscription to same channel
    auto resp2 = client.send_command({"SUBSCRIBE", "channel1"});
    
    // Should still show count 1 (not 2) - subscribing twice to same channel doesn't increase count
    if (resp2.find(":1\r\n") == std::string::npos) {
        return test_failed("SUBSCRIBE duplicate", "Duplicate subscription should still show count 1");
    }
    
    return test_passed("SUBSCRIBE same channel twice");
}

// ============= Stage 2: PUBLISH Command (No Subscribers) =============

bool test_publish_no_subscribers() {
    std::cout << "\n=== Testing PUBLISH with no subscribers ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("PUBLISH no subs", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("PUBLISH no subs", "Could not connect to server");
    }
    
    auto resp = client.send_command({"PUBLISH", "channel1", "hello"});
    
    // Should return :0\r\n (0 clients received the message)
    if (resp != ":0\r\n") {
        return test_failed("PUBLISH no subs", "Expected :0\\r\\n, got: " + escape_string(resp));
    }
    
    return test_passed("PUBLISH with no subscribers");
}

bool test_publish_different_message_types() {
    std::cout << "\n=== Testing PUBLISH with different message types ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("PUBLISH types", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("PUBLISH types", "Could not connect to server");
    }
    
    // Test empty message
    auto resp = client.send_command({"PUBLISH", "channel1", ""});
    if (resp != ":0\r\n") {
        return test_failed("PUBLISH empty", "Expected :0\\r\\n for empty message");
    }
    
    // Test message with spaces
    resp = client.send_command({"PUBLISH", "channel1", "hello world"});
    if (resp != ":0\r\n") {
        return test_failed("PUBLISH spaces", "Expected :0\\r\\n for message with spaces");
    }
    
    // Test long message
    std::string long_msg(1000, 'x');
    resp = client.send_command({"PUBLISH", "channel1", long_msg});
    if (resp != ":0\r\n") {
        return test_failed("PUBLISH long", "Expected :0\\r\\n for long message");
    }
    
    return test_passed("PUBLISH different message types");
}

// ============= Stage 3: Message Delivery (Single Subscriber) =============

bool test_message_delivery_single_subscriber() {
    std::cout << "\n=== Testing message delivery to single subscriber ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Delivery single", "Could not start server");
    }
    
    RedisClient subscriber;
    if (!subscriber.connect()) {
        return test_failed("Delivery single", "Could not connect subscriber");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Delivery single", "Could not connect publisher");
    }
    
    // Subscribe
    auto resp = subscriber.send_command({"SUBSCRIBE", "channel1"});
    if (resp.find("subscribe") == std::string::npos) {
        return test_failed("Delivery single", "Subscription failed");
    }
    
    // Give a moment for subscription to register
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Publish message
    resp = publisher.send_command({"PUBLISH", "channel1", "hello"});
    if (resp != ":1\r\n") {
        return test_failed("Delivery single", "Expected :1\\r\\n (1 subscriber), got: " + escape_string(resp));
    }
    
    // Subscriber should receive the message
    // Expected format: *3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n$5\r\nhello\r\n
    resp = subscriber.recv_with_timeout(1000);
    
    if (resp.find("*3\r\n") == std::string::npos) {
        return test_failed("Delivery single", "Expected array of 3 elements in message");
    }
    
    if (resp.find("message") == std::string::npos) {
        return test_failed("Delivery single", "Missing 'message' keyword");
    }
    
    if (resp.find("channel1") == std::string::npos) {
        return test_failed("Delivery single", "Missing channel name in message");
    }
    
    if (resp.find("hello") == std::string::npos) {
        return test_failed("Delivery single", "Missing message content");
    }
    
    return test_passed("Message delivery to single subscriber");
}

bool test_message_not_delivered_to_other_channels() {
    std::cout << "\n=== Testing message not delivered to other channels ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("No cross-delivery", "Could not start server");
    }
    
    RedisClient subscriber;
    if (!subscriber.connect()) {
        return test_failed("No cross-delivery", "Could not connect subscriber");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("No cross-delivery", "Could not connect publisher");
    }
    
    // Subscribe to channel1
    subscriber.send_command({"SUBSCRIBE", "channel1"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Publish to channel2 (different channel)
    auto resp = publisher.send_command({"PUBLISH", "channel2", "hello"});
    if (resp != ":0\r\n") {
        return test_failed("No cross-delivery", "Expected :0\\r\\n (no subscribers to channel2)");
    }
    
    // Subscriber should NOT receive any message
    subscriber.set_nonblocking();
    resp = subscriber.recv_with_timeout(500);
    subscriber.set_blocking();
    
    if (resp.find("message") != std::string::npos) {
        return test_failed("No cross-delivery", "Subscriber should not receive message from different channel");
    }
    
    return test_passed("Message not delivered to other channels");
}

// ============= Stage 4: Message Delivery (Multiple Subscribers) =============

bool test_message_delivery_multiple_subscribers() {
    std::cout << "\n=== Testing message delivery to multiple subscribers ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Delivery multiple", "Could not start server");
    }
    
    RedisClient sub1, sub2, sub3;
    if (!sub1.connect() || !sub2.connect() || !sub3.connect()) {
        return test_failed("Delivery multiple", "Could not connect subscribers");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Delivery multiple", "Could not connect publisher");
    }
    
    // All subscribe to same channel
    sub1.send_command({"SUBSCRIBE", "channel1"});
    sub2.send_command({"SUBSCRIBE", "channel1"});
    sub3.send_command({"SUBSCRIBE", "channel1"});
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Publish message
    auto resp = publisher.send_command({"PUBLISH", "channel1", "broadcast"});
    if (resp != ":3\r\n") {
        return test_failed("Delivery multiple", "Expected :3\\r\\n (3 subscribers), got: " + escape_string(resp));
    }
    
    // All subscribers should receive the message
    auto msg1 = sub1.recv_with_timeout(1000);
    auto msg2 = sub2.recv_with_timeout(1000);
    auto msg3 = sub3.recv_with_timeout(1000);
    
    if (msg1.find("broadcast") == std::string::npos) {
        return test_failed("Delivery multiple", "Subscriber 1 did not receive message");
    }
    
    if (msg2.find("broadcast") == std::string::npos) {
        return test_failed("Delivery multiple", "Subscriber 2 did not receive message");
    }
    
    if (msg3.find("broadcast") == std::string::npos) {
        return test_failed("Delivery multiple", "Subscriber 3 did not receive message");
    }
    
    return test_passed("Message delivery to multiple subscribers");
}

bool test_subscriber_receives_only_subscribed_channels() {
    std::cout << "\n=== Testing subscriber receives only subscribed channels ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Selective delivery", "Could not start server");
    }
    
    RedisClient sub1, sub2;
    if (!sub1.connect() || !sub2.connect()) {
        return test_failed("Selective delivery", "Could not connect subscribers");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Selective delivery", "Could not connect publisher");
    }
    
    // sub1 subscribes to channel1, sub2 subscribes to channel2
    sub1.send_command({"SUBSCRIBE", "channel1"});
    sub2.send_command({"SUBSCRIBE", "channel2"});
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Publish to channel1
    auto resp = publisher.send_command({"PUBLISH", "channel1", "msg1"});
    if (resp != ":1\r\n") {
        return test_failed("Selective delivery", "Expected 1 subscriber for channel1");
    }
    
    // Only sub1 should receive
    auto msg1 = sub1.recv_with_timeout(1000);
    if (msg1.find("msg1") == std::string::npos) {
        return test_failed("Selective delivery", "sub1 should receive message");
    }
    
    // sub2 should NOT receive
    sub2.set_nonblocking();
    auto msg2 = sub2.recv_with_timeout(500);
    sub2.set_blocking();
    
    if (msg2.find("msg1") != std::string::npos) {
        return test_failed("Selective delivery", "sub2 should not receive message from channel1");
    }
    
    return test_passed("Subscriber receives only subscribed channels");
}

// ============= Stage 5: Multiple Messages =============


bool test_multiple_messages_in_sequence() {
    std::cout << "\n=== Testing multiple messages in sequence ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Multiple messages", "Could not start server");
    }
    
    RedisClient subscriber;
    if (!subscriber.connect()) {
        return test_failed("Multiple messages", "Could not connect subscriber");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Multiple messages", "Could not connect publisher");
    }
    
    // Subscribe
    subscriber.send_command({"SUBSCRIBE", "channel1"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Publish messages with delays
    publisher.send_command({"PUBLISH", "channel1", "msg1"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    publisher.send_command({"PUBLISH", "channel1", "msg2"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    publisher.send_command({"PUBLISH", "channel1", "msg3"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Receive all 3 messages
    std::string all_messages;
    for (int i = 0; i < 5; i++) {
        auto msg = subscriber.recv_with_timeout(500);
        all_messages += msg;
        if (msg.empty()) break;
    }
    
    if (all_messages.find("msg1") == std::string::npos) {
        return test_failed("Multiple messages", "Missing msg1");
    }
    
    if (all_messages.find("msg2") == std::string::npos) {
        return test_failed("Multiple messages", "Missing msg2");
    }
    
    if (all_messages.find("msg3") == std::string::npos) {
        return test_failed("Multiple messages", "Missing msg3");
    }
    
    return test_passed("Multiple messages in sequence");
}


bool test_rapid_fire_messages() {
    std::cout << "\n=== Testing rapid-fire message delivery ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Rapid fire", "Could not start server");
    }
    
    RedisClient subscriber;
    if (!subscriber.connect()) {
        return test_failed("Rapid fire", "Could not connect subscriber");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Rapid fire", "Could not connect publisher");
    }
    
    subscriber.send_command({"SUBSCRIBE", "channel1"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Send 10 messages rapidly
    for (int i = 0; i < 10; i++) {
        publisher.send_command({"PUBLISH", "channel1", "msg" + std::to_string(i)});
    }
    
    // Give time for messages to arrive
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Collect all received data
    std::string all_data;
    for (int i = 0; i < 15; i++) {
        auto msg = subscriber.recv_with_timeout(200);
        if (msg.empty()) break;
        all_data += msg;
    }
    
    // Count how many unique messages we received
    int received_count = 0;
    for (int i = 0; i < 10; i++) {
        std::string expected = "msg" + std::to_string(i);
        if (all_data.find(expected) != std::string::npos) {
            received_count++;
        }
    }
    
    if (received_count != 10) {
        return test_failed("Rapid fire", "Expected 10 messages, received: " + std::to_string(received_count));
    }
    
    return test_passed("Rapid-fire message delivery");
}


// ============= Stage 6: Subscriber to Multiple Channels =============

bool test_subscriber_multiple_channels() {
    std::cout << "\n=== Testing subscriber to multiple channels ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Sub multiple channels", "Could not start server");
    }
    
    RedisClient subscriber;
    if (!subscriber.connect()) {
        return test_failed("Sub multiple channels", "Could not connect subscriber");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Sub multiple channels", "Could not connect publisher");
    }
    
    // Subscribe to multiple channels
    subscriber.send_command({"SUBSCRIBE", "channel1", "channel2"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Publish to both channels
    publisher.send_command({"PUBLISH", "channel1", "msg1"});
    publisher.send_command({"PUBLISH", "channel2", "msg2"});
    
    // Should receive both messages
    auto resp1 = subscriber.recv_with_timeout(1000);
    auto resp2 = subscriber.recv_with_timeout(1000);
    
    bool got_msg1 = (resp1.find("msg1") != std::string::npos) || (resp2.find("msg1") != std::string::npos);
    bool got_msg2 = (resp1.find("msg2") != std::string::npos) || (resp2.find("msg2") != std::string::npos);
    
    if (!got_msg1) {
        return test_failed("Sub multiple channels", "Did not receive msg1");
    }
    
    if (!got_msg2) {
        return test_failed("Sub multiple channels", "Did not receive msg2");
    }
    
    return test_passed("Subscriber to multiple channels");
}

// ============= Stage 7: Commands in Subscribe Mode =============

bool test_regular_commands_after_subscribe() {
    std::cout << "\n=== Testing regular commands after SUBSCRIBE ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Commands after sub", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Commands after sub", "Could not connect");
    }
    
    // Subscribe
    client.send_command({"SUBSCRIBE", "channel1"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Try to execute a regular command (SET)
    auto resp = client.send_command({"SET", "key", "value"});
    
    // Should receive an error indicating client is in subscribe mode
    // Redis returns: -ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context
    if (resp.find("-ERR") == std::string::npos) {
        return test_failed("Commands after sub", "Should reject regular commands in subscribe mode");
    }
    
    return test_passed("Regular commands rejected after SUBSCRIBE");
}

bool test_allowed_commands_in_subscribe_mode() {
    std::cout << "\n=== Testing allowed commands in subscribe mode ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Allowed in sub mode", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Allowed in sub mode", "Could not connect");
    }
    
    // Subscribe
    client.send_command({"SUBSCRIBE", "channel1"});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // PING should be allowed
    auto resp = client.send_command({"PING"});
    if (resp.find("PONG") == std::string::npos && resp.find("pong") == std::string::npos) {
        return test_failed("Allowed in sub mode", "PING should be allowed in subscribe mode");
    }
    
    // SUBSCRIBE should be allowed (subscribing to more channels)
    resp = client.send_command({"SUBSCRIBE", "channel2"});
    if (resp.find("subscribe") == std::string::npos) {
        return test_failed("Allowed in sub mode", "SUBSCRIBE should be allowed");
    }
    
    return test_passed("Allowed commands in subscribe mode");
}

// ============= Edge Cases and Integration Tests =============

bool test_disconnect_removes_subscriber() {
    std::cout << "\n=== Testing disconnect removes subscriber ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Disconnect cleanup", "Could not start server");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Disconnect cleanup", "Could not connect publisher");
    }
    
    {
        RedisClient subscriber;
        if (!subscriber.connect()) {
            return test_failed("Disconnect cleanup", "Could not connect subscriber");
        }
        
        subscriber.send_command({"SUBSCRIBE", "channel1"});
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // Publish should show 1 subscriber
        auto resp = publisher.send_command({"PUBLISH", "channel1", "test"});
        if (resp != ":1\r\n") {
            return test_failed("Disconnect cleanup", "Expected 1 subscriber initially");
        }
        
        // subscriber goes out of scope and disconnects
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Now publish should show 0 subscribers
    auto resp = publisher.send_command({"PUBLISH", "channel1", "test"});
    if (resp != ":0\r\n") {
        return test_failed("Disconnect cleanup", "Expected 0 subscribers after disconnect, got: " + escape_string(resp));
    }
    
    return test_passed("Disconnect removes subscriber");
}

bool test_empty_channel_name() {
    std::cout << "\n=== Testing empty channel name ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Empty channel", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("Empty channel", "Could not connect");
    }
    
    // Try to subscribe to empty channel name
    auto resp = client.send_command({"SUBSCRIBE", ""});
    
    // Should either accept it (empty string is a valid channel name) or reject it
    // Redis actually accepts empty channel names, so we should too
    if (resp.find("subscribe") == std::string::npos && resp.find("-ERR") == std::string::npos) {
        return test_failed("Empty channel", "Unexpected response: " + escape_string(resp));
    }
    
    return test_passed("Empty channel name handling");
}

bool test_special_characters_in_channel_names() {
    std::cout << "\n=== Testing special characters in channel names ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("Special chars", "Could not start server");
    }
    
    RedisClient subscriber;
    if (!subscriber.connect()) {
        return test_failed("Special chars", "Could not connect subscriber");
    }
    
    RedisClient publisher;
    if (!publisher.connect()) {
        return test_failed("Special chars", "Could not connect publisher");
    }
    
    // Test channel names with special characters
    std::string channel = "channel:with:colons";
    subscriber.send_command({"SUBSCRIBE", channel});
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    auto resp = publisher.send_command({"PUBLISH", channel, "test"});
    if (resp != ":1\r\n") {
        return test_failed("Special chars", "Special characters should be allowed in channel names");
    }
    
    auto msg = subscriber.recv_with_timeout(1000);
    if (msg.find(channel) == std::string::npos || msg.find("test") == std::string::npos) {
        return test_failed("Special chars", "Message not received with special char channel");
    }
    
    return test_passed("Special characters in channel names");
}

int main() {
    std::cout << "=====================================" << std::endl;
    std::cout << "    Redis PUB/SUB Test Suite" << std::endl;
    std::cout << "=====================================" << std::endl;
    
    int passed = 0;
    int total = 0;
    
    std::vector<std::pair<std::string, bool(*)()>> tests = {
        // Stage 1: SUBSCRIBE
        {"SUBSCRIBE single channel", test_subscribe_single_channel},
        {"SUBSCRIBE multiple channels", test_subscribe_multiple_channels},
        {"SUBSCRIBE same channel twice", test_subscribe_same_channel_twice},
        
        // Stage 2: PUBLISH (no subscribers)
        {"PUBLISH with no subscribers", test_publish_no_subscribers},
        {"PUBLISH different message types", test_publish_different_message_types},
        
        // Stage 3: Message delivery (single)
        {"Message delivery to single subscriber", test_message_delivery_single_subscriber},
        {"Message not delivered to other channels", test_message_not_delivered_to_other_channels},
        
        // Stage 4: Message delivery (multiple)
        {"Message delivery to multiple subscribers", test_message_delivery_multiple_subscribers},
        {"Subscriber receives only subscribed channels", test_subscriber_receives_only_subscribed_channels},
        
        // Stage 5: Multiple messages
        {"Multiple messages in sequence", test_multiple_messages_in_sequence},
        {"Rapid-fire message delivery", test_rapid_fire_messages},
        
        // Stage 6: Subscriber to multiple channels
        {"Subscriber to multiple channels", test_subscriber_multiple_channels},
        
        // Stage 7: Commands in subscribe mode
        {"Regular commands rejected after SUBSCRIBE", test_regular_commands_after_subscribe},
        {"Allowed commands in subscribe mode", test_allowed_commands_in_subscribe_mode},
        
        // Edge cases
        {"Disconnect removes subscriber", test_disconnect_removes_subscriber},
        {"Empty channel name handling", test_empty_channel_name},
        {"Special characters in channel names", test_special_characters_in_channel_names}
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
        std::cout << "✓ All PUB/SUB tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "✗ Some tests failed" << std::endl;
        return 1;
    }
}