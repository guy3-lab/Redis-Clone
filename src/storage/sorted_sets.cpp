#include "sorted_sets.h"
#include "storage.h"
#include "../protocol/resp_parser.h"

std::unordered_map<std::string, SortedSet> sortedSetStorage;

std::string handle_zadd(const std::vector<std::string>& args) {
    if (args.size() < 4 || args.size() % 2 != 0) {
        return encode_error("ERR wrong number of arguments for 'zadd' command");
    }
    
    std::string key = args[1];
    int added_count = 0;
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    // process pairs
    for (size_t i = 2; i < args.size(); i += 2) {
        double score = std::stod(args[i]);
        std::string member = args[i + 1];
        
        // check if member already exists
        bool is_new = (sortedSetStorage[key].member_scores.count(member) == 0);
        
        if (is_new) {
            added_count++;
        } else {
            // remove old entry from sorted set
            double old_score = sortedSetStorage[key].member_scores[member];
            sortedSetStorage[key].sorted_members.erase({old_score, member});
        }
        
        // add member
        sortedSetStorage[key].member_scores[member] = score;
        sortedSetStorage[key].sorted_members.insert({score, member});
    }
    
    return encode_integer(added_count);
}

std::string handle_zrank(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'zrank' command");
    }
    
    std::string key = args[1];
    std::string member = args[2];
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    // check if key exists
    if (sortedSetStorage.count(key) == 0) {
        return "$-1\r\n";
    }
    
    // check if member exists
    if (sortedSetStorage[key].member_scores.count(member) == 0) {
        return "$-1\r\n";
    }
    
    // find rank by iterating through sorted members
    int rank = 0;
    double target_score = sortedSetStorage[key].member_scores[member];
    
    for (const auto& sm : sortedSetStorage[key].sorted_members) {
        if (sm.score == target_score && sm.member == member) {
            return encode_integer(rank);
        }
        rank++;
    }
    
    // shouldnt reach here
    return "$-1\r\n";
}

std::string handle_zrange(const std::vector<std::string>& args) {
    if (args.size() < 4) {
        return encode_error("ERR wrong number of arguments for 'zrange' command");
    }
    
    std::string key = args[1];
    int start = std::stoi(args[2]);
    int stop = std::stoi(args[3]);
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    // check if key exists
    if (sortedSetStorage.count(key) == 0) {
        return "*0\r\n";
    }
    
    int size = sortedSetStorage[key].sorted_members.size();
    
    // handle negative indices
    if (start < 0) {
        start = size + start;
        if (start < 0) start = 0;
    }
    
    if (stop < 0) {
        stop = size + stop;
        if (stop < 0) stop = -1;  // will return empty
    }
    
    // check bounds
    if (start >= size || stop < 0 || start > stop) {
        return "*0\r\n";
    }
    
    if (stop >= size) {
        stop = size - 1;
    }
    
    // collect members in range
    std::vector<std::string> result;
    int idx = 0;
    
    for (const auto& sm : sortedSetStorage[key].sorted_members) {
        if (idx >= start && idx <= stop) {
            result.push_back(sm.member);
        }
        idx++;
        if (idx > stop) break;
    }
    
    // build response
    std::string response = "*" + std::to_string(result.size()) + "\r\n";
    for (const auto& member : result) {
        response += encode_bulk_string(member);
    }
    
    return response;
}

std::string handle_zcard(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'zcard' command");
    }
    
    std::string key = args[1];
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    if (sortedSetStorage.count(key) == 0) {
        return encode_integer(0);
    }
    
    int size = sortedSetStorage[key].sorted_members.size();
    return encode_integer(size);
}

std::string handle_zscore(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'zscore' command");
    }
    
    std::string key = args[1];
    std::string member = args[2];
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    // check if key exists
    if (sortedSetStorage.count(key) == 0) {
        return "$-1\r\n";
    }
    
    // check if member exists
    if (sortedSetStorage[key].member_scores.count(member) == 0) {
        return "$-1\r\n";
    }
    
    double score = sortedSetStorage[key].member_scores[member];
    std::string score_str = std::to_string(score);
    
    // remove trailing zeros after decimal point
    size_t dot_pos = score_str.find('.');
    if (dot_pos != std::string::npos) {
        size_t last_nonzero = score_str.find_last_not_of('0');
        if (last_nonzero > dot_pos) {
            score_str = score_str.substr(0, last_nonzero + 1);
        }
        // remove trailing dot if no decimals
        if (score_str.back() == '.') {
            score_str.pop_back();
        }
    }
    
    return encode_bulk_string(score_str);
}

std::string handle_zrem(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'zrem' command");
    }
    
    std::string key = args[1];
    int removed_count = 0;
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    // check if key exists
    if (sortedSetStorage.count(key) == 0) {
        return encode_integer(0);
    }
    
    // rem each member specified
    for (size_t i = 2; i < args.size(); i++) {
        std::string member = args[i];
        
        // check if member exists
        if (sortedSetStorage[key].member_scores.count(member) > 0) {
            double score = sortedSetStorage[key].member_scores[member];
            
            // rem from both structures
            sortedSetStorage[key].sorted_members.erase({score, member});
            sortedSetStorage[key].member_scores.erase(member);
            
            removed_count++;
        }
    }
    
    // clean up empty sorted sets
    if (sortedSetStorage[key].sorted_members.empty()) {
        sortedSetStorage.erase(key);
    }
    
    return encode_integer(removed_count);
}