#include "storage.h"
#include "../protocol/resp_parser.h"

std::unordered_map<std::string, StorageValue> storageMap;
std::mutex storage_mutex;


std::string handle_set(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'set' command");
    }
    
    // debug logging
    std::cerr << "DEBUG handle_set: Setting key=" << args[1] << " value=" << args[2] << std::endl;
    
    if (args.size() >= 5) {
        std::string px_flag = args[3];
        std::transform(px_flag.begin(), px_flag.end(), px_flag.begin(), ::tolower);
        if (px_flag == "px") {
            int input_expiry_ms = std::stoi(args[4]);
            long long final_expiry_ms = get_current_time_ms() + input_expiry_ms;
            std::lock_guard<std::mutex> lock(storage_mutex);
            storageMap[args[1]] = {args[2], final_expiry_ms};
            std::cerr << "DEBUG handle_set: Stored with expiry" << std::endl;
        }
    } else {
        std::lock_guard<std::mutex> lock(storage_mutex);
        storageMap[args[1]] = {args[2], 0};
        std::cerr << "DEBUG handle_set: Stored without expiry, map size=" << storageMap.size() << std::endl;
    }
    
    return encode_simple_string("OK");
}

// and add debug to handle_get
std::string handle_get(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'get' command");
    }
    
    std::cerr << "DEBUG handle_get: Getting key=" << args[1] << std::endl;
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    std::cerr << "DEBUG handle_get: Storage map size=" << storageMap.size() << std::endl;
    std::cerr << "DEBUG handle_get: Key exists=" << (storageMap.count(args[1]) > 0) << std::endl;
    
    if (storageMap.count(args[1]) > 0) {
        if (storageMap[args[1]].expiry_ms > 0) {
            if (get_current_time_ms() < storageMap[args[1]].expiry_ms) {
                std::cerr << "DEBUG handle_get: Returning value=" << storageMap[args[1]].value << std::endl;
                return encode_bulk_string(storageMap[args[1]].value);
            } else {
                std::cerr << "DEBUG handle_get: Key expired" << std::endl;
                storageMap.erase(args[1]);
                return "$-1\r\n";
            }
        } else {
            std::cerr << "DEBUG handle_get: Returning value=" << storageMap[args[1]].value << std::endl;
            return encode_bulk_string(storageMap[args[1]].value);
        }
    }
    
    std::cerr << "DEBUG handle_get: Key not found" << std::endl;
    return "$-1\r\n";
}

std::string handle_incr(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'incr' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    long long value = 0;
    bool exists = false;
    bool is_numeric = true;
    
    if (storageMap.count(args[1]) > 0) {
        exists = true;
        if (storageMap[args[1]].expiry_ms > 0 && 
            get_current_time_ms() >= storageMap[args[1]].expiry_ms) {
            storageMap.erase(args[1]);
            exists = false;
        } else {
            if (!is_valid_integer(storageMap[args[1]].value, value)) {
                is_numeric = false;
            }
        }
    }
    
    if (!exists) {
        storageMap[args[1]] = {"1", 0};
        return encode_integer(1);
    } else if (!is_numeric) {
        return encode_error("ERR value is not an integer or out of range");
    } else {
        value++;
        storageMap[args[1]].value = std::to_string(value);
        return encode_integer(value);
    }
}

std::string handle_type(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'type' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    if (storageMap.count(args[1]) > 0) {
        if (storageMap[args[1]].expiry_ms > 0 && 
            get_current_time_ms() >= storageMap[args[1]].expiry_ms) {
            storageMap.erase(args[1]);
            return encode_simple_string("none");
        } else {
            return encode_simple_string("string");
        }
    }
    else if (listStorage.count(args[1]) > 0) {
        return encode_simple_string("list");
    }
    else if (streamStorage.count(args[1]) > 0) {
        return encode_simple_string("stream");
    }
    else if (sortedSetStorage.count(args[1]) > 0) {
        return encode_simple_string("zset");
    }
    
    return encode_simple_string("none");
}