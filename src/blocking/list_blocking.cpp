#include "blocking.h"
#include "../protocol/resp_parser.h"
#include "../storage/storage.h"
#include "../utils/utils.h"

std::mutex blocked_clients_mutex;
std::vector<BlockedClient> blocked_clients;
std::mutex blocked_fds_mutex;
std::set<int> blocked_fds;
std::condition_variable blocked_cv;
std::atomic<bool> should_check_blocked{false};

bool is_client_blocked(int client_fd) {
    std::lock_guard<std::mutex> lock(blocked_fds_mutex);
    return blocked_fds.count(client_fd) > 0;
}

void notify_blocked_clients() {
    should_check_blocked = true;
    blocked_cv.notify_all();
}

std::string handle_blocking_pop(const std::vector<std::string>& args, int client_fd, const std::string& command) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for '" + command + "' command");
    }
    
    // parse timeout (last argument)
    double timeout_seconds = std::stod(args[args.size() - 1]);
    long long timeout_ms = (timeout_seconds == 0) ? 0 : (long long)(timeout_seconds * 1000);
    
    // extract keys (all arguments except command and timeout)
    std::vector<std::string> keys;
    for (size_t i = 1; i < args.size() - 1; i++) {
        keys.push_back(args[i]);
    }
    
    // try to pop from first available list
    bool found = false;
    std::string popped_key;
    std::string popped_value;
    
    {
        std::lock_guard<std::mutex> lock(storage_mutex);
        for (const auto& key : keys) {
            if (listStorage.count(key) > 0 && !listStorage[key].empty()) {
                popped_key = key;
                
                if (command == "BLPOP") {
                    popped_value = listStorage[key].front();
                    listStorage[key].pop_front();
                } else { // BRPOP
                    popped_value = listStorage[key].back();
                    listStorage[key].pop_back();
                }
                
                if (listStorage[key].empty()) {
                    listStorage.erase(key);
                }
                
                found = true;
                break;
            }
        }
    }
    
    if (found) {
        // send immediate response
        std::string response = "*2\r\n";
        response += encode_bulk_string(popped_key);
        response += encode_bulk_string(popped_value);
        return response;
    }
    
    // block the client
    BlockedClient bc;
    bc.client_fd = client_fd;
    bc.keys = keys;
    bc.timeout_ms = timeout_ms;
    bc.start_time_ms = get_current_time_ms();
    bc.command_type = command;
    
    {
        std::lock_guard<std::mutex> lock(blocked_clients_mutex);
        blocked_clients.push_back(bc);
    }
    
    {
        std::lock_guard<std::mutex> lock(blocked_fds_mutex);
        blocked_fds.insert(client_fd);
    }
    
    // don't send response now client is blocked
    return "";
}

void process_blocked_clients() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        std::lock_guard<std::mutex> lock(blocked_clients_mutex);
        auto now = get_current_time_ms();
        
        for (auto it = blocked_clients.begin(); it != blocked_clients.end();) {
            bool should_unblock = false;
            std::string unblock_key;
            std::string popped_value;
            
            // check timeout
            if (it->timeout_ms > 0 && (now - it->start_time_ms) >= it->timeout_ms) {
                // timeout reached
                std::string response = "$-1\r\n";
                send(it->client_fd, response.c_str(), response.size(), 0);
                
                {
                    std::lock_guard<std::mutex> fd_lock(blocked_fds_mutex);
                    blocked_fds.erase(it->client_fd);
                }
                
                it = blocked_clients.erase(it);
                continue;
            }
            
            // check if any key has elements
            {
                std::lock_guard<std::mutex> storage_lock(storage_mutex);
                for (const auto& key : it->keys) {
                    if (listStorage.count(key) > 0 && !listStorage[key].empty()) {
                        unblock_key = key;
                        
                        if (it->command_type == "BLPOP") {
                            popped_value = listStorage[key].front();
                            listStorage[key].pop_front();
                        } else if (it->command_type == "BRPOP") {
                            popped_value = listStorage[key].back();
                            listStorage[key].pop_back();
                        }
                        
                        if (listStorage[key].empty()) {
                            listStorage.erase(key);
                        }
                        
                        should_unblock = true;
                        break;
                    }
                }
            }
            
            if (should_unblock) {
                // send response
                std::string response = "*2\r\n";
                response += encode_bulk_string(unblock_key);
                response += encode_bulk_string(popped_value);
                send(it->client_fd, response.c_str(), response.size(), 0);
                
                {
                    std::lock_guard<std::mutex> fd_lock(blocked_fds_mutex);
                    blocked_fds.erase(it->client_fd);
                }
                
                it = blocked_clients.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void cleanup_client(int client_fd) {
    // remove from blocked clients if disconnected
    {
        std::lock_guard<std::mutex> lock(blocked_clients_mutex);
        blocked_clients.erase(
            std::remove_if(blocked_clients.begin(), blocked_clients.end(),
                [client_fd](const BlockedClient& bc) { return bc.client_fd == client_fd; }),
            blocked_clients.end()
        );
    }
    
    {
        std::lock_guard<std::mutex> lock(blocked_fds_mutex);
        blocked_fds.erase(client_fd);
    }
}