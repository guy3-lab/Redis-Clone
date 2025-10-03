#include "blocking.h"
#include "../protocol/resp_parser.h"
#include "../storage/storage.h"
#include "../utils/utils.h"

std::mutex xread_blocked_mutex;
std::vector<XReadBlockedClient> xread_blocked_clients;

void notify_xread_blocked_clients() {
    // just to wake up blocked XREAD clients when new stream entries are added
    // this is called after XADD operations
}

void process_xread_blocked_clients() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        
        std::lock_guard<std::mutex> lock(xread_blocked_mutex);
        auto now = get_current_time_ms();
        
        for (auto it = xread_blocked_clients.begin(); it != xread_blocked_clients.end();) {
            bool should_unblock = false;
            std::string response;
            
            // check timeout
            if (it->timeout_ms > 0 && (now - it->start_time_ms) >= it->timeout_ms) {
                response = "*-1\r\n";  // Null bulk string for timeout
                send(it->client_fd, response.c_str(), response.size(), 0);
                
                {
                    std::lock_guard<std::mutex> fd_lock(blocked_fds_mutex);
                    blocked_fds.erase(it->client_fd);
                }
                
                it = xread_blocked_clients.erase(it);
                continue;
            }
            
            // check if any stream has new data
            std::vector<std::pair<std::string, std::vector<StreamEntry>>> results;
            
            {
                std::lock_guard<std::mutex> storage_lock(storage_mutex);
                for (size_t i = 0; i < it->stream_keys.size(); i++) {
                    const auto& key = it->stream_keys[i];
                    const auto& after_id = it->stream_ids[i];
                    
                    if (streamStorage.count(key) > 0) {
                        std::vector<StreamEntry> entries;
                        for (const auto& entry : streamStorage[key]) {
                            if (compare_stream_ids(entry.id, after_id) > 0) {
                                entries.push_back(entry);
                            }
                        }
                        if (!entries.empty()) {
                            results.push_back({key, entries});
                        }
                    }
                }
            }
            
            if (!results.empty()) {
                // format response
                response = "*" + std::to_string(results.size()) + "\r\n";
                for (const auto& [stream_key, entries] : results) {
                    response += "*2\r\n";
                    response += encode_bulk_string(stream_key);
                    response += "*" + std::to_string(entries.size()) + "\r\n";
                    
                    for (const auto& entry : entries) {
                        response += "*2\r\n";
                        response += encode_bulk_string(entry.id);
                        response += "*" + std::to_string(entry.fields.size() * 2) + "\r\n";
                        for (const auto& [k, v] : entry.fields) {
                            response += encode_bulk_string(k);
                            response += encode_bulk_string(v);
                        }
                    }
                }
                
                send(it->client_fd, response.c_str(), response.size(), 0);
                
                {
                    std::lock_guard<std::mutex> fd_lock(blocked_fds_mutex);
                    blocked_fds.erase(it->client_fd);
                }
                
                it = xread_blocked_clients.erase(it);
            } else {
                ++it;
            }
        }
    }
}