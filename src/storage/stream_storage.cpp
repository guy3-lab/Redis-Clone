#include "storage.h"
#include "../protocol/resp_parser.h"
#include "../blocking/blocking.h"

std::unordered_map<std::string, std::vector<StreamEntry>> streamStorage;

std::pair<long long, long long> parse_stream_id(const std::string& id) {
    size_t dash_pos = id.find('-');
    if (dash_pos == std::string::npos) {
        return {std::stoll(id), 0};
    }
    long long ms = std::stoll(id.substr(0, dash_pos));
    long long seq = std::stoll(id.substr(dash_pos + 1));
    return {ms, seq};
}

int compare_stream_ids(const std::string& id1, const std::string& id2) {
    auto [ms1, seq1] = parse_stream_id(id1);
    auto [ms2, seq2] = parse_stream_id(id2);
    
    if (ms1 != ms2) return (ms1 > ms2) ? 1 : -1;
    if (seq1 != seq2) return (seq1 > seq2) ? 1 : -1;
    return 0;
}

std::string handle_xadd(const std::vector<std::string>& args) {
    if (args.size() < 5 || (args.size() % 2) == 0) {
        return encode_error("ERR wrong number of arguments for 'xadd' command");
    }
    
    std::string stream_key = args[1];
    std::string requested_id = args[2];
    
    std::vector<std::pair<std::string, std::string>> fields;
    for (size_t i = 3; i < args.size(); i += 2) {
        fields.push_back({args[i], args[i + 1]});
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    std::string actual_id;
    
    if (requested_id == "*") {
        long long ms = get_current_time_ms();
        long long seq = 0;
        
        if (streamStorage.count(stream_key) > 0 && !streamStorage[stream_key].empty()) {
            auto last_id = streamStorage[stream_key].back().id;
            auto [last_ms, last_seq] = parse_stream_id(last_id);
            
            if (ms == last_ms) {
                seq = last_seq + 1;
            }
        }
        
        actual_id = std::to_string(ms) + "-" + std::to_string(seq);
    }
    else if (requested_id.find('*') != std::string::npos) {
        size_t star_pos = requested_id.find('*');
        std::string ms_str = requested_id.substr(0, star_pos - 1);
        long long ms = std::stoll(ms_str);
        long long seq = 0;
        
        if (ms == 0) {
            seq = 1;
        } else if (streamStorage.count(stream_key) > 0 && !streamStorage[stream_key].empty()) {
            auto last_id = streamStorage[stream_key].back().id;
            auto [last_ms, last_seq] = parse_stream_id(last_id);
            
            if (ms == last_ms) {
                seq = last_seq + 1;
            }
        }
        
        actual_id = ms_str + "-" + std::to_string(seq);
    }
    else {
        actual_id = requested_id;
        
        if (actual_id == "0-0") {
            return encode_error("ERR The ID specified in XADD must be greater than 0-0");
        }
        
        if (streamStorage.count(stream_key) > 0 && !streamStorage[stream_key].empty()) {
            auto last_id = streamStorage[stream_key].back().id;
            if (compare_stream_ids(actual_id, last_id) <= 0) {
                return encode_error("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
    }
    
    StreamEntry entry;
    entry.id = actual_id;
    entry.fields = fields;
    
    streamStorage[stream_key].push_back(entry);
    
    return encode_bulk_string(actual_id);
}

std::string handle_xrange(const std::vector<std::string>& args) {
    if (args.size() < 4) {
        return encode_error("ERR wrong number of arguments for 'xrange' command");
    }
    
    std::string stream_key = args[1];
    std::string start_id = args[2];
    std::string end_id = args[3];
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    if (streamStorage.count(stream_key) == 0) {
        return "*0\r\n";
    }
    
    std::vector<StreamEntry> results;
    
    for (const auto& entry : streamStorage[stream_key]) {
        bool include = false;
        
        if (start_id == "-") {
            include = true;
        } else {
            std::string compare_start = start_id;
            if (start_id.find('-') == std::string::npos) {
                compare_start += "-0";
            }
            if (compare_stream_ids(entry.id, compare_start) >= 0) {
                include = true;
            }
        }
        
        if (include && end_id != "+") {
            std::string compare_end = end_id;
            if (end_id.find('-') == std::string::npos) {
                compare_end += "-18446744073709551615";
            }
            if (compare_stream_ids(entry.id, compare_end) > 0) {
                include = false;
            }
        }
        
        if (include) {
            results.push_back(entry);
        }
    }
    
    std::string response = "*" + std::to_string(results.size()) + "\r\n";
    for (const auto& entry : results) {
        response += "*2\r\n";
        response += encode_bulk_string(entry.id);
        response += "*" + std::to_string(entry.fields.size() * 2) + "\r\n";
        for (const auto& [k, v] : entry.fields) {
            response += encode_bulk_string(k);
            response += encode_bulk_string(v);
        }
    }
    
    return response;
}

std::string handle_xread(const std::vector<std::string>& args, int client_fd) {
    // dw blocking is handled
    return "*-1\r\n";
}