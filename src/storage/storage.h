#ifndef STORAGE_H
#define STORAGE_H

#include "../server.h"
#include "../utils/utils.h"
#include "sorted_sets.h"

struct StorageValue {
    std::string value;
    long long expiry_ms;
};

struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
};

// global storage
extern std::unordered_map<std::string, StorageValue> storageMap;
extern std::mutex storage_mutex;
extern std::unordered_map<std::string, std::deque<std::string>> listStorage;
extern std::unordered_map<std::string, std::vector<StreamEntry>> streamStorage;

// kv operations
std::string handle_set(const std::vector<std::string>& args);
std::string handle_get(const std::vector<std::string>& args);
std::string handle_incr(const std::vector<std::string>& args);
std::string handle_type(const std::vector<std::string>& args);

// list operations
std::string handle_lpush(const std::vector<std::string>& args);
std::string handle_rpush(const std::vector<std::string>& args);
std::string handle_lpop(const std::vector<std::string>& args);
std::string handle_rpop(const std::vector<std::string>& args);
std::string handle_lrange(const std::vector<std::string>& args);
std::string handle_llen(const std::vector<std::string>& args);

// stream operations
std::string handle_xadd(const std::vector<std::string>& args);
std::string handle_xrange(const std::vector<std::string>& args);
std::string handle_xread(const std::vector<std::string>& args, int client_fd);

// stream helpers
std::pair<long long, long long> parse_stream_id(const std::string& id);
int compare_stream_ids(const std::string& id1, const std::string& id2);

#endif