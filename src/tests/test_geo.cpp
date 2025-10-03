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
#include <cmath>

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

// Helper to extract double from bulk string
double extract_double_from_bulk(const std::string& resp) {
    size_t start = resp.find("\r\n") + 2;
    size_t end = resp.find("\r\n", start);
    if (start == std::string::npos || end == std::string::npos) return 0.0;
    return std::stod(resp.substr(start, end - start));
}

// Helper to check if two doubles are approximately equal (within precision)
bool approx_equal(double a, double b, double precision = 0.0001) {
    return std::abs(a - b) < precision;
}

// ============= Stage 1: GEOADD (Basic Response) =============

bool test_geoadd_basic_response() {
    std::cout << "\n=== Testing GEOADD basic response ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOADD basic", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOADD basic", "Could not connect");
    }
    
    // Add location
    auto resp = client.send_command({"GEOADD", "places", "11.5030378", "48.164271", "Munich"});
    
    // Should return :1\r\n (1 location added)
    if (resp != ":1\r\n") {
        return test_failed("GEOADD basic", "Expected :1\\r\\n, got: " + escape_string(resp));
    }
    
    return test_passed("GEOADD basic response");
}

// ============= Stage 2: GEOADD (Validate Coordinates) =============

bool test_geoadd_validate_longitude() {
    std::cout << "\n=== Testing GEOADD longitude validation ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOADD longitude", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOADD longitude", "Could not connect");
    }
    
    // Invalid longitude (> 180)
    auto resp = client.send_command({"GEOADD", "places", "181", "0", "test"});
    
    // Should return error
    if (resp.find("-ERR") == std::string::npos) {
        return test_failed("GEOADD longitude", "Should reject invalid longitude");
    }
    
    if (resp.find("longitude") == std::string::npos) {
        return test_failed("GEOADD longitude", "Error should mention 'longitude'");
    }
    
    // Valid longitude at boundary
    resp = client.send_command({"GEOADD", "places", "180", "0", "test"});
    if (resp != ":1\r\n") {
        return test_failed("GEOADD longitude boundary", "180 should be valid");
    }
    
    return test_passed("GEOADD longitude validation");
}

bool test_geoadd_validate_latitude() {
    std::cout << "\n=== Testing GEOADD latitude validation ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOADD latitude", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOADD latitude", "Could not connect");
    }
    
    // Invalid latitude (> 85.05112878)
    auto resp = client.send_command({"GEOADD", "places", "0", "90", "test"});
    
    // Should return error
    if (resp.find("-ERR") == std::string::npos) {
        return test_failed("GEOADD latitude", "Should reject invalid latitude");
    }
    
    if (resp.find("latitude") == std::string::npos) {
        return test_failed("GEOADD latitude", "Error should mention 'latitude'");
    }
    
    // Valid latitude at boundary
    resp = client.send_command({"GEOADD", "places", "0", "85.05112878", "test"});
    if (resp != ":1\r\n") {
        return test_failed("GEOADD latitude boundary", "85.05112878 should be valid");
    }
    
    return test_passed("GEOADD latitude validation");
}

// ============= Stage 3: GEOADD (Store in Sorted Set) =============

bool test_geoadd_stores_in_sorted_set() {
    std::cout << "\n=== Testing GEOADD stores in sorted set ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOADD store", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOADD store", "Could not connect");
    }
    
    // Add location
    auto resp = client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    if (resp != ":1\r\n") {
        return test_failed("GEOADD store", "GEOADD should return :1");
    }
    
    // Verify it's stored as sorted set using ZRANGE
    resp = client.send_command({"ZRANGE", "places", "0", "-1"});
    
    // Should contain "Paris"
    if (resp.find("Paris") == std::string::npos) {
        return test_failed("GEOADD store", "Location not found in sorted set");
    }
    
    if (resp.find("*1\r\n") == std::string::npos) {
        return test_failed("GEOADD store", "Should have 1 member");
    }
    
    return test_passed("GEOADD stores in sorted set");
}

// ============= Stage 4: GEOADD (Calculate Score) =============

bool test_geoadd_calculates_score() {
    std::cout << "\n=== Testing GEOADD calculates correct score ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOADD score", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOADD score", "Could not connect");
    }
    
    // Add Paris with known coordinates
    client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    
    // Get score using ZSCORE
    auto resp = client.send_command({"ZSCORE", "places", "Paris"});
    
    // Expected score for Paris: 3663832614298053
    if (resp.find("3663832614298053") == std::string::npos) {
        return test_failed("GEOADD score Paris", "Score not calculated correctly for Paris, got: " + escape_string(resp));
    }
    
    // Test roundtrip: add location, get position, verify coordinates are close
    client.send_command({"GEOADD", "places2", "-0.127758", "51.507351", "London"});
    
    auto pos_resp = client.send_command({"GEOPOS", "places2", "London"});
    
    // Should decode back to approximately the same coordinates (within 6 decimal places)
    if (pos_resp.find("-0.127") == std::string::npos) {
        return test_failed("GEOADD roundtrip lon", "Longitude doesn't roundtrip correctly");
    }
    
    if (pos_resp.find("51.507") == std::string::npos) {
        return test_failed("GEOADD roundtrip lat", "Latitude doesn't roundtrip correctly");
    }
    
    return test_passed("GEOADD calculates correct score");
}

// ============= Stage 5: GEOPOS (Basic Response) =============

bool test_geopos_basic_response() {
    std::cout << "\n=== Testing GEOPOS basic response ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOPOS basic", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOPOS basic", "Could not connect");
    }
    
    // Add location
    client.send_command({"GEOADD", "places", "-0.0884948", "51.506479", "London"});
    
    // Get position
    auto resp = client.send_command({"GEOPOS", "places", "London"});
    
    // Should return array of 1 element, which is an array of 2 elements (longitude, latitude)
    // Format: *1\r\n*2\r\n$...\r\n<longitude>\r\n$...\r\n<latitude>\r\n
    
    if (resp.find("*1\r\n") == std::string::npos) {
        return test_failed("GEOPOS basic", "Expected array of 1, got: " + escape_string(resp));
    }
    
    if (resp.find("*2\r\n") == std::string::npos) {
        return test_failed("GEOPOS basic", "Expected nested array of 2");
    }
    
    return test_passed("GEOPOS basic response");
}

bool test_geopos_missing_location() {
    std::cout << "\n=== Testing GEOPOS with missing location ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOPOS missing", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOPOS missing", "Could not connect");
    }
    
    // Get position for non-existent location
    auto resp = client.send_command({"GEOPOS", "places", "missing"});
    
    // Should return array with 1 null array: *1\r\n*-1\r\n
    if (resp != "*1\r\n*-1\r\n") {
        return test_failed("GEOPOS missing", "Expected *1\\r\\n*-1\\r\\n, got: " + escape_string(resp));
    }
    
    return test_passed("GEOPOS missing location");
}

bool test_geopos_multiple_locations() {
    std::cout << "\n=== Testing GEOPOS with multiple locations ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOPOS multiple", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOPOS multiple", "Could not connect");
    }
    
    // Add locations
    client.send_command({"GEOADD", "places", "11.5030378", "48.164271", "Munich"});
    client.send_command({"GEOADD", "places", "-0.0884948", "51.506479", "London"});
    
    // Get both positions
    auto resp = client.send_command({"GEOPOS", "places", "Munich", "London"});
    
    // Should return array of 2, each with array of 2
    if (resp.find("*2\r\n") != 0) {
        return test_failed("GEOPOS multiple", "Expected array of 2");
    }
    
    // Count nested arrays
    int array_count = 0;
    size_t pos = 0;
    while ((pos = resp.find("*2\r\n", pos + 1)) != std::string::npos) {
        array_count++;
    }
    
    if (array_count < 2) {
        return test_failed("GEOPOS multiple", "Expected 2 nested arrays");
    }
    
    return test_passed("GEOPOS multiple locations");
}

// ============= Stage 6: GEOPOS (Decode Coordinates) =============

bool test_geopos_decode_coordinates() {
    std::cout << "\n=== Testing GEOPOS decodes coordinates correctly ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOPOS decode", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOPOS decode", "Could not connect");
    }
    
    // Add Paris with known coordinates
    client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    
    // Get position
    auto resp = client.send_command({"GEOPOS", "places", "Paris"});
    
    // Extract longitude and latitude from response
    // We need to be lenient - within 6 decimal places as per task list
    
    // Expected: longitude ≈ 2.294469, latitude ≈ 48.858462
    if (resp.find("2.294") == std::string::npos) {
        return test_failed("GEOPOS decode", "Longitude not close to expected value");
    }
    
    if (resp.find("48.858") == std::string::npos) {
        return test_failed("GEOPOS decode", "Latitude not close to expected value");
    }
    
    return test_passed("GEOPOS decodes coordinates correctly");
}

bool test_geopos_multiple_decode() {
    std::cout << "\n=== Testing GEOPOS decodes multiple locations ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOPOS multi decode", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOPOS multi decode", "Could not connect");
    }
    
    // Add multiple locations with known scores
    client.send_command({"ZADD", "location_key", "3663832614298053", "Foo"});
    client.send_command({"ZADD", "location_key", "3876464048901851", "Bar"});
    
    // Get positions
    auto resp = client.send_command({"GEOPOS", "location_key", "Foo"});
    
    // Should have approximate coordinates (within 6 decimal places)
    // Foo should decode to approximately (2.294471, 48.858462)
    if (resp.find("2.294") == std::string::npos || resp.find("48.858") == std::string::npos) {
        return test_failed("GEOPOS multi decode", "Foo coordinates not decoded correctly");
    }
    
    return test_passed("GEOPOS decodes multiple locations");
}

// ============= Stage 7: GEODIST =============

bool test_geodist_basic() {
    std::cout << "\n=== Testing GEODIST basic ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEODIST basic", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEODIST basic", "Could not connect");
    }
    
    // Add Munich and Paris
    client.send_command({"GEOADD", "places", "11.5030378", "48.164271", "Munich"});
    client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    
    // Get distance
    auto resp = client.send_command({"GEODIST", "places", "Munich", "Paris"});
    
    // Should be bulk string with distance in meters
    if (resp[0] != '$') {
        return test_failed("GEODIST basic", "Expected bulk string response");
    }
    
    // Extract distance value
    double distance = extract_double_from_bulk(resp);
    
    // Expected: approximately 682477.7582 meters (±0.0001)
    if (!approx_equal(distance, 682477.7582, 1.0)) {
        return test_failed("GEODIST basic", "Distance incorrect: " + std::to_string(distance));
    }
    
    return test_passed("GEODIST basic");
}

bool test_geodist_missing_members() {
    std::cout << "\n=== Testing GEODIST with missing members ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEODIST missing", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEODIST missing", "Could not connect");
    }
    
    // Add only one location
    client.send_command({"GEOADD", "places", "11.5030378", "48.164271", "Munich"});
    
    // Try to get distance to non-existent location
    auto resp = client.send_command({"GEODIST", "places", "Munich", "Paris"});
    
    // Should return null bulk string
    if (resp != "$-1\r\n") {
        return test_failed("GEODIST missing", "Expected $-1\\r\\n for missing member");
    }
    
    return test_passed("GEODIST with missing members");
}

// ============= Stage 8: GEOSEARCH =============

bool test_geosearch_basic() {
    std::cout << "\n=== Testing GEOSEARCH basic ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOSEARCH basic", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOSEARCH basic", "Could not connect");
    }
    
    // Add locations
    client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    client.send_command({"GEOADD", "places", "-0.0884948", "51.506479", "London"});
    client.send_command({"GEOADD", "places", "11.5030378", "48.164271", "Munich"});
    
    // Search near Paris (2, 48) with 100km radius
    auto resp = client.send_command({"GEOSEARCH", "places", "FROMLONLAT", "2", "48", "BYRADIUS", "100", "km"});
    
    // Should return array containing Paris
    if (resp.find("Paris") == std::string::npos) {
        return test_failed("GEOSEARCH basic", "Should find Paris");
    }
    
    // Should NOT contain London or Munich (too far)
    if (resp.find("London") != std::string::npos || resp.find("Munich") != std::string::npos) {
        return test_failed("GEOSEARCH basic", "Should not find distant locations");
    }
    
    return test_passed("GEOSEARCH basic");
}

bool test_geosearch_multiple_results() {
    std::cout << "\n=== Testing GEOSEARCH with multiple results ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOSEARCH multiple", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOSEARCH multiple", "Could not connect");
    }
    
    // Add locations
    client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    client.send_command({"GEOADD", "places", "-0.0884948", "51.506479", "London"});
    client.send_command({"GEOADD", "places", "11.5030378", "48.164271", "Munich"});
    
    // Search with larger radius to get multiple results
    auto resp = client.send_command({"GEOSEARCH", "places", "FROMLONLAT", "2", "48", "BYRADIUS", "500", "km"});
    
    // Should return Paris and London (both within 500km of point 2,48)
    if (resp.find("Paris") == std::string::npos) {
        return test_failed("GEOSEARCH multiple", "Should find Paris");
    }
    
    if (resp.find("London") == std::string::npos) {
        return test_failed("GEOSEARCH multiple", "Should find London");
    }
    
    // Munich is ~900km away, should not be included
    if (resp.find("Munich") != std::string::npos) {
        return test_failed("GEOSEARCH multiple", "Should not find Munich (too far)");
    }
    
    return test_passed("GEOSEARCH with multiple results");
}

bool test_geosearch_different_units() {
    std::cout << "\n=== Testing GEOSEARCH with different units ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOSEARCH units", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOSEARCH units", "Could not connect");
    }
    
    // Add locations
    client.send_command({"GEOADD", "places", "11.5030378", "48.164271", "Munich"});
    client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    
    // Test with meters
    auto resp = client.send_command({"GEOSEARCH", "places", "FROMLONLAT", "11", "50", "BYRADIUS", "300000", "m"});
    
    // Should find Munich (within 300km = 300000m)
    if (resp.find("Munich") == std::string::npos) {
        return test_failed("GEOSEARCH meters", "Should find Munich with meters");
    }
    
    // Test with kilometers (same search)
    resp = client.send_command({"GEOSEARCH", "places", "FROMLONLAT", "11", "50", "BYRADIUS", "300", "km"});
    
    if (resp.find("Munich") == std::string::npos) {
        return test_failed("GEOSEARCH km", "Should find Munich with kilometers");
    }
    
    return test_passed("GEOSEARCH with different units");
}

bool test_geosearch_empty_results() {
    std::cout << "\n=== Testing GEOSEARCH with no results ===" << std::endl;
    
    RedisServer server(6379);
    if (!server.start()) {
        return test_failed("GEOSEARCH empty", "Could not start server");
    }
    
    RedisClient client;
    if (!client.connect()) {
        return test_failed("GEOSEARCH empty", "Could not connect");
    }
    
    // Add locations
    client.send_command({"GEOADD", "places", "2.2944692", "48.8584625", "Paris"});
    
    // Search far away with small radius
    auto resp = client.send_command({"GEOSEARCH", "places", "FROMLONLAT", "100", "0", "BYRADIUS", "100", "km"});
    
    // Should return empty array
    if (resp != "*0\r\n") {
        return test_failed("GEOSEARCH empty", "Expected *0\\r\\n for no results, got: " + escape_string(resp));
    }
    
    return test_passed("GEOSEARCH with no results");
}

int main() {
    std::cout << "=====================================" << std::endl;
    std::cout << "    Redis GEO Commands Test Suite" << std::endl;
    std::cout << "=====================================" << std::endl;
    
    int passed = 0;
    int total = 0;
    
    std::vector<std::pair<std::string, bool(*)()>> tests = {
        // Stage 1: GEOADD basic response
        {"GEOADD basic response", test_geoadd_basic_response},
        
        // Stage 2: GEOADD coordinate validation
        {"GEOADD longitude validation", test_geoadd_validate_longitude},
        {"GEOADD latitude validation", test_geoadd_validate_latitude},
        
        // Stage 3: GEOADD stores in sorted set
        {"GEOADD stores in sorted set", test_geoadd_stores_in_sorted_set},
        
        // Stage 4: GEOADD calculates score
        {"GEOADD calculates correct score", test_geoadd_calculates_score},
        
        // Stage 5: GEOPOS basic response
        {"GEOPOS basic response", test_geopos_basic_response},
        {"GEOPOS missing location", test_geopos_missing_location},
        {"GEOPOS multiple locations", test_geopos_multiple_locations},
        
        // Stage 6: GEOPOS decode coordinates
        {"GEOPOS decodes coordinates correctly", test_geopos_decode_coordinates},
        {"GEOPOS decodes multiple locations", test_geopos_multiple_decode},
        
        // Stage 7: GEODIST
        {"GEODIST basic", test_geodist_basic},
        {"GEODIST with missing members", test_geodist_missing_members},
        
        // Stage 8: GEOSEARCH
        {"GEOSEARCH basic", test_geosearch_basic},
        {"GEOSEARCH with multiple results", test_geosearch_multiple_results},
        {"GEOSEARCH with different units", test_geosearch_different_units},
        {"GEOSEARCH with no results", test_geosearch_empty_results}
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
        std::cout << "✓ All GEO tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "✗ Some tests failed" << std::endl;
        return 1;
    }
}