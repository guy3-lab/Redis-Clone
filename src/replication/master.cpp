#include "replication.h"
#include "../protocol/resp_parser.h"
#include "../utils/utils.h"

const unsigned char empty_rdb_hex[] = {
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31,
    0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76,
    0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa,
    0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69,
    0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69,
    0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08,
    0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2,
    0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66,
    0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0,
    0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2
};

std::map<int, std::unique_ptr<ConnectedReplica>> connected_replicas;
std::mutex replicas_mutex;
std::condition_variable ack_cv;

void process_replica_responses(int replica_fd) {
    // this thread is no longer needed since handle_client handles everything
    // but we keep it for compatibility so just make it do nothing
    debug_log << "DEBUG process_replica_responses: Started for fd=" << replica_fd 
              << " (now handled by handle_client)" << std::endl;
    
    // just sleep until the replica disconnects
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        std::lock_guard<std::mutex> lock(replicas_mutex);
        if (!connected_replicas.count(replica_fd) || !connected_replicas[replica_fd]->active) {
            break;
        }
    }
    
    debug_log << "DEBUG process_replica_responses: Exiting for fd=" << replica_fd << std::endl;
}

std::string handle_psync(int client_fd, const std::vector<std::string>& message) {
    if (message.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'psync' command");
    }
    
    debug_log << "DEBUG handle_psync: Starting for client_fd=" << client_fd << std::endl;
    
    // check how many replicas we already have
    {
        std::lock_guard<std::mutex> lock(replicas_mutex);
        debug_log << "DEBUG handle_psync: Currently have " << connected_replicas.size() << " replicas" << std::endl;
        for (const auto& [fd, replica] : connected_replicas) {
            debug_log << "  Replica fd=" << fd << ", active=" << replica->active << std::endl;
        }
    }
    
    // send FULLRESYNC response
    std::string fullresync = "+FULLRESYNC " + repl_config.replication_id + " 0\r\n";
    send(client_fd, fullresync.c_str(), fullresync.size(), 0);
    debug_log << "DEBUG handle_psync: Sent FULLRESYNC" << std::endl;
    
    // send RDB file
    std::string rdb_data(reinterpret_cast<const char*>(empty_rdb_hex), sizeof(empty_rdb_hex));
    std::string rdb_response = "$" + std::to_string(rdb_data.size()) + "\r\n" + rdb_data;
    send(client_fd, rdb_response.c_str(), rdb_response.size(), 0);
    debug_log << "DEBUG handle_psync: Sent RDB file" << std::endl;
    
    // enable TCP_NODELAY for low latency
    int flag = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
    
    // add replica to connected list
    {
        std::lock_guard<std::mutex> lock(replicas_mutex);
        
        // check if this fd is already in the map, shouldnt be
        if (connected_replicas.count(client_fd)) {
            debug_log << "DEBUG handle_psync: WARNING - client_fd " << client_fd 
                      << " already in connected_replicas!" << std::endl;
        }
        
        auto replica = std::make_unique<ConnectedReplica>();
        replica->fd = client_fd;
        replica->listening_port = "";
        replica->last_ping_time = get_current_time_ms();
        replica->active = true;
        replica->ack_received = false;
        
        // DO NOT access connection_offset cuz it doesn't exist yet
        // replica->connection_offset = repl_config.replication_offset.load(); // faulty line
        
        // start response handler thread for this replica
        replica->response_thread = std::thread(process_replica_responses, client_fd);
        
        connected_replicas[client_fd] = std::move(replica);
        debug_log << "PSYNC_TRACK: Added fd=" << client_fd << " to connected_replicas" << std::endl;
        debug_log << "PSYNC_TRACK: Total replicas now: " << connected_replicas.size() << std::endl;
        debug_log << "DEBUG handle_psync: Added replica to connected list, now have " 
                  << connected_replicas.size() << " replicas" << std::endl;
    }
    
    return "";
}



std::string handle_wait(const std::vector<std::string>& message) {
    debug_log << "\n=== WAIT COMMAND START ===" << std::endl;
    
    if (message.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'wait' command");
    }
    
    int num_replicas_needed = std::stoi(message[1]);
    int timeout_ms = std::stoi(message[2]);
    
    debug_log << "WAIT_PARAMS: num_needed=" << num_replicas_needed 
              << " timeout=" << timeout_ms << "ms" << std::endl;
    
    // count total replicas
    int total_replicas = 0;
    {
        std::lock_guard<std::mutex> lock(replicas_mutex);
        debug_log << "WAIT_REPLICAS: Checking " << connected_replicas.size() << " connections" << std::endl;
        for (const auto& [fd, replica] : connected_replicas) {
            debug_log << "  fd=" << fd 
                      << " active=" << replica->active
                      << " ack_offset=" << replica->ack_offset
                      << " ack_received=" << replica->ack_received << std::endl;
            if (replica->active) {
                total_replicas++;
            }
        }
    }
    
    debug_log << "WAIT_COUNT: " << total_replicas << " active replicas" << std::endl;
    
    if (total_replicas == 0) {
        debug_log << "WAIT_RETURN: 0 (no replicas)" << std::endl;
        return encode_integer(0);
    }
    
    // get current offsets
    long long repl_offset = repl_config.replication_offset.load();
    long long last_write = repl_config.last_write_offset.load();
    
    debug_log << "WAIT_OFFSETS: replication_offset=" << repl_offset 
              << " last_write_offset=" << last_write << std::endl;
    
    // if no writes, all replicas are in sync
    if (last_write == 0) {
        debug_log << "WAIT_RETURN: " << std::min(num_replicas_needed, total_replicas) 
                  << " (no writes, all in sync)" << std::endl;
        return encode_integer(total_replicas);
    }
    
    // send REPLCONF GETACK to all replicas
    std::vector<std::string> getack_cmd = {"REPLCONF", "GETACK", "*"};
    std::string getack_resp = encode_as_resp_array(getack_cmd);

    debug_log << "WAIT_GETACK: Sending GETACK to replicas" << std::endl;

    std::unique_lock<std::mutex> lock(replicas_mutex);
    for (const auto& [fd, replica] : connected_replicas) {
        if (replica->active) {
            replica->ack_received = false;
            int sent = send(fd, getack_resp.c_str(), getack_resp.size(), MSG_NOSIGNAL);
            debug_log << "WAIT_GETACK: Sent to fd=" << fd
                      << " bytes=" << sent << std::endl;
        }
    }

    // a replica counts once it has ACKed an offset at or past the last write
    auto count_acked = [&] {
        int acked = 0;
        for (const auto& [fd, replica] : connected_replicas) {
            if (replica->active && replica->ack_received && replica->ack_offset >= last_write) {
                acked++;
            }
        }
        return acked;
    };

    // sleep until enough replicas ACK or the timeout hits (0 = wait forever)
    // the ACK handler in handle_client notifies ack_cv, and waiting releases replicas_mutex so it can get in
    int target = std::min(num_replicas_needed, total_replicas);
    auto enough_acked = [&] { return count_acked() >= target; };
    if (timeout_ms > 0) {
        ack_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), enough_acked);
    } else {
        ack_cv.wait(lock, enough_acked);
    }
    int acked_count = count_acked();

    debug_log << "WAIT_RETURN: " << acked_count << " (out of " << total_replicas
              << " replicas)" << std::endl;
    debug_log << "=== WAIT COMMAND END ===" << std::endl;
    
    return encode_integer(acked_count);
}


void propagate_to_replicas(const std::vector<std::string>& command) {
    std::string encoded = encode_as_resp_array(command);
    
    debug_log << "DEBUG PROPAGATE: Propagating command: ";
    for (const auto& arg : command) {
        debug_log << arg << " ";
    }
    debug_log << "\n";
    
    std::lock_guard<std::mutex> lock(replicas_mutex);
    
    debug_log << "DEBUG PROPAGATE: Sending to " << connected_replicas.size() << " replicas\n";
    
    // send to all active replicas
    int sent_count = 0;
    for (auto it = connected_replicas.begin(); it != connected_replicas.end();) {
        if (it->second->active) {
            int result = send(it->first, encoded.c_str(), encoded.size(), MSG_NOSIGNAL);
            if (result < 0) {
                debug_log << "DEBUG PROPAGATE: Failed to send to replica fd=" << it->first << "\n";
                it->second->active = false;
                ++it;
            } else {
                debug_log << "DEBUG PROPAGATE: Sent " << result << " bytes to replica fd=" << it->first << "\n";
                sent_count++;
                // again enable TCP_NODELAY for low latency
                int flag = 1;
                setsockopt(it->first, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(int));
                ++it;
            }
        } else {
            ++it;
        }
    }
    
    // update offsets
    long long old_offset = repl_config.replication_offset.load();
    repl_config.replication_offset += encoded.size();
    repl_config.last_write_offset = repl_config.replication_offset.load();
    
    debug_log << "DEBUG PROPAGATE: Sent to " << sent_count << " replicas, "
              << "offset updated from " << old_offset << " to " << repl_config.replication_offset.load() << "\n";
}

// add a function to send GETACK to replicas
// called by wait cmd or test
void send_getack_to_replica(int replica_fd) {
    std::vector<std::string> getack_cmd = {"REPLCONF", "GETACK", "*"};
    std::string getack_resp = encode_as_resp_array(getack_cmd);
    
    int result = send(replica_fd, getack_resp.c_str(), getack_resp.size(), MSG_NOSIGNAL);
    if (result > 0) {
        debug_log << "DEBUG: Sent GETACK to replica fd=" << replica_fd << std::endl;
        // update replication offset for this command
        repl_config.replication_offset += getack_resp.size();
    }
}


std::string handle_info_replication() {
    std::string info_content;
    
    if (repl_config.is_replica) {
        info_content = "role:slave";
    } else {
        int slave_count = 0;
        {
            std::lock_guard<std::mutex> lock(replicas_mutex);
            debug_log << "DEBUG INFO: Checking " << connected_replicas.size() << " replicas" << std::endl;
            for (const auto& [fd, replica] : connected_replicas) {
                debug_log << "DEBUG INFO: Replica fd=" << fd << ", active=" << replica->active << std::endl;
                if (replica->active) {
                    slave_count++;
                }
            }
        }
        
        debug_log << "DEBUG INFO: Reporting " << slave_count << " connected slaves" << std::endl;
        
        info_content = "role:master\r\n";
        info_content += "connected_slaves:" + std::to_string(slave_count) + "\r\n";
        info_content += "master_replid:" + repl_config.replication_id + "\r\n";
        info_content += "master_repl_offset:" + std::to_string(repl_config.replication_offset.load());
    }
    
    return encode_bulk_string(info_content);
}

// this was purely to check if replconf is being called
std::string handle_replconf(const std::vector<std::string>& message, bool is_replica) {
    debug_log << "DEBUG handle_replconf: called with " << message.size() << " args, is_replica=" << is_replica << std::endl;
    for (size_t i = 0; i < message.size(); i++) {
        debug_log << "  arg[" << i << "]: " << message[i] << std::endl;
    }
    
    if (message.size() < 3) {
        debug_log << "DEBUG handle_replconf: returning error - wrong number of arguments" << std::endl;
        return encode_error("ERR wrong number of arguments");
    }
    
    std::string subcommand = message[1];
    std::transform(subcommand.begin(), subcommand.end(), subcommand.begin(), ::tolower);
    debug_log << "DEBUG handle_replconf: subcommand=" << subcommand << std::endl;
    
    if (subcommand == "listening-port" || subcommand == "capa") {
        debug_log << "DEBUG handle_replconf: returning OK for " << subcommand << std::endl;
        return encode_simple_string("OK");
    } else if (subcommand == "getack" && is_replica) {
        debug_log << "DEBUG handle_replconf: GETACK received in normal command handler (shouldn't happen!)" << std::endl;
        long long offset = replica_offset.load();
        std::vector<std::string> ack_response = {"REPLCONF", "ACK", std::to_string(offset)};
        auto response = encode_as_resp_array(ack_response);
        debug_log << "DEBUG handle_replconf: sending ACK with offset=" << offset << std::endl;
        return response;
    }
    
    debug_log << "DEBUG handle_replconf: returning default OK" << std::endl;
    return encode_simple_string("OK");
}