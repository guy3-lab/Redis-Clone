#include "storage.h"
#include "../protocol/resp_parser.h"

std::unordered_map<std::string, std::deque<std::string>> listStorage;

// list cmd handlers

std::string handle_lpush(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'lpush' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    for (size_t i = 2; i < args.size(); i++) {
        listStorage[args[1]].push_front(args[i]);
    }
    int size = listStorage[args[1]].size();
    
    return encode_integer(size);
}

std::string handle_rpush(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'rpush' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    for (size_t i = 2; i < args.size(); i++) {
        listStorage[args[1]].push_back(args[i]);
    }
    int size = listStorage[args[1]].size();
    
    return encode_integer(size);
}

std::string handle_lpop(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'lpop' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    if (listStorage.count(args[1]) == 0 || listStorage[args[1]].empty()) {
        return "$-1\r\n";
    }
    
    if (args.size() == 2) {
        std::string frontVal = listStorage[args[1]].front();
        listStorage[args[1]].pop_front();
        if (listStorage[args[1]].empty()) {
            listStorage.erase(args[1]);
        }
        return encode_bulk_string(frontVal);
    } else {
        int count = std::stoi(args[2]);
        std::string result = "*";
        std::vector<std::string> values;
        
        while (count > 0 && !listStorage[args[1]].empty()) {
            values.push_back(listStorage[args[1]].front());
            listStorage[args[1]].pop_front();
            count--;
        }
        
        if (listStorage[args[1]].empty()) {
            listStorage.erase(args[1]);
        }
        
        result += std::to_string(values.size()) + "\r\n";
        for (const auto& val : values) {
            result += encode_bulk_string(val);
        }
        return result;
    }
}

std::string handle_rpop(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'rpop' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    if (listStorage.count(args[1]) == 0 || listStorage[args[1]].empty()) {
        return "$-1\r\n";
    }
    
    if (args.size() == 2) {
        std::string backVal = listStorage[args[1]].back();
        listStorage[args[1]].pop_back();
        if (listStorage[args[1]].empty()) {
            listStorage.erase(args[1]);
        }
        return encode_bulk_string(backVal);
    } else {
        int count = std::stoi(args[2]);
        std::string result = "*";
        std::vector<std::string> values;
        
        while (count > 0 && !listStorage[args[1]].empty()) {
            values.push_back(listStorage[args[1]].back());
            listStorage[args[1]].pop_back();
            count--;
        }
        
        if (listStorage[args[1]].empty()) {
            listStorage.erase(args[1]);
        }
        
        result += std::to_string(values.size()) + "\r\n";
        for (const auto& val : values) {
            result += encode_bulk_string(val);
        }
        return result;
    }
}

std::string handle_lrange(const std::vector<std::string>& args) {
    if (args.size() < 4) {
        return encode_error("ERR wrong number of arguments for 'lrange' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    if (listStorage.count(args[1]) == 0) {
        return "*0\r\n";
    }
    
    int listSize = listStorage[args[1]].size();
    int start = std::stoi(args[2]);
    int stop = std::stoi(args[3]);
    
    if (start < 0) {
        start = std::max(0, listSize + start);
    }
    if (stop < 0) {
        stop = listSize + stop;
    }
    
    if (start > stop || start >= listSize) {
        return "*0\r\n";
    }
    
    stop = std::min(stop, listSize - 1);
    
    std::string result = "*" + std::to_string(stop - start + 1) + "\r\n";
    for (int i = start; i <= stop; i++) {
        result += encode_bulk_string(listStorage[args[1]][i]);
    }
    
    return result;
}

std::string handle_llen(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'llen' command");
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    int size = listStorage.count(args[1]) == 0 ? 0 : listStorage[args[1]].size();
    return encode_integer(size);
}