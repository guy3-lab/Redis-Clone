#include "command_handler.h"
#include "../protocol/resp_parser.h"
#include "../rdb/rdb_parser.h"
#include "../storage/storage.h"

std::string handle_config_get(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'config get' command");
    }
    
    std::string param = args[2];
    std::transform(param.begin(), param.end(), param.begin(), ::tolower);
    
    std::string response = "*2\r\n";
    
    if (param == "dir") {
        response += encode_bulk_string("dir");
        response += encode_bulk_string(rdb_config.dir);
    } else if (param == "dbfilename") {
        response += encode_bulk_string("dbfilename");
        response += encode_bulk_string(rdb_config.dbfilename);
    } else {
        // return empty array unknown parameter
        return "*0\r\n";
    }
    
    return response;
}

std::string handle_keys(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'keys' command");
    }
    
    std::string pattern = args[1];
    
    // only support "*" pattern for now
    if (pattern != "*") {
        return encode_error("ERR only '*' pattern is supported");
    }
    
    std::vector<std::string> keys;
    long long current_time = get_current_time_ms();
    
    {
        std::lock_guard<std::mutex> lock(storage_mutex);
        
        // collect all non-expired keys
        for (const auto& [key, value] : storageMap) {
            // Check if key has expired
            if (value.expiry_ms == 0 || value.expiry_ms > current_time) {
                keys.push_back(key);
            }
        }
    }
    
    // build response
    std::string response = "*" + std::to_string(keys.size()) + "\r\n";
    for (const auto& key : keys) {
        response += encode_bulk_string(key);
    }
    
    return response;
}