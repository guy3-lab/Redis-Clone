#include "replication.h"
#include "../protocol/resp_parser.h"
#include "../commands/command_handler.h"
#include "../storage/storage.h"

int master_connection_fd = -1;
std::mutex master_conn_mutex;
std::atomic<long long> replica_offset{0};

bool connect_to_master() {
    std::lock_guard<std::mutex> lock(master_conn_mutex);
    
    debug_log << "DEBUG REPLICA: Connecting to master at " 
              << repl_config.master_host << ":" << repl_config.master_port << "\n";
    
    if (master_connection_fd >= 0) {
        close(master_connection_fd);
        master_connection_fd = -1;
    }
    
    master_connection_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (master_connection_fd < 0) {
        debug_log << "DEBUG REPLICA: Failed to create socket\n";
        return false;
    }
    
    struct sockaddr_in master_addr;
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(repl_config.master_port);
    
    struct hostent* host = gethostbyname(repl_config.master_host.c_str());
    if (!host) {
        debug_log << "DEBUG REPLICA: Failed to resolve host " << repl_config.master_host << "\n";
        close(master_connection_fd);
        master_connection_fd = -1;
        return false;
    }
    
    memcpy(&master_addr.sin_addr, host->h_addr_list[0], host->h_length);
    
    if (connect(master_connection_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        debug_log << "DEBUG REPLICA: Failed to connect, errno=" << errno << "\n";
        close(master_connection_fd);
        master_connection_fd = -1;
        return false;
    }
    
    debug_log << "DEBUG REPLICA: Connected to master, fd=" << master_connection_fd << "\n";
    
    char buffer[4096];
    memset(buffer, 0, sizeof(buffer));
    
    // send PING
    std::string ping_cmd = encode_as_resp_array({"PING"});
    debug_log << "DEBUG REPLICA: Sending PING\n";
    send(master_connection_fd, ping_cmd.c_str(), ping_cmd.size(), 0);
    int bytes = recv(master_connection_fd, buffer, sizeof(buffer), 0);
    debug_log << "DEBUG REPLICA: PING response: " << bytes << " bytes\n";
    
    // send REPLCONF listening-port
    std::string replconf1 = encode_as_resp_array({"REPLCONF", "listening-port", std::to_string(repl_config.listening_port)});
    debug_log << "DEBUG REPLICA: Sending REPLCONF listening-port\n";
    send(master_connection_fd, replconf1.c_str(), replconf1.size(), 0);
    memset(buffer, 0, sizeof(buffer));
    bytes = recv(master_connection_fd, buffer, sizeof(buffer), 0);
    debug_log << "DEBUG REPLICA: REPLCONF listening-port response: " << bytes << " bytes\n";
    
    // send REPLCONF capa psync2
    std::string replconf2 = encode_as_resp_array({"REPLCONF", "capa", "psync2"});
    debug_log << "DEBUG REPLICA: Sending REPLCONF capa psync2\n";
    send(master_connection_fd, replconf2.c_str(), replconf2.size(), 0);
    memset(buffer, 0, sizeof(buffer));
    bytes = recv(master_connection_fd, buffer, sizeof(buffer), 0);
    debug_log << "DEBUG REPLICA: REPLCONF capa response: " << bytes << " bytes\n";
    
    // send PSYNC
    std::string psync = encode_as_resp_array({"PSYNC", "?", "-1"});
    debug_log << "DEBUG REPLICA: Sending PSYNC ? -1\n";
    send(master_connection_fd, psync.c_str(), psync.size(), 0);
    
    // receive FULLRESYNC response AND RDB file
    memset(buffer, 0, sizeof(buffer));
    bytes = recv(master_connection_fd, buffer, sizeof(buffer), 0);
    debug_log << "DEBUG REPLICA: PSYNC response: " << bytes << " bytes, first 100 chars: ";
    for (int i = 0; i < std::min(bytes, 100); i++) {
        if (buffer[i] >= 32 && buffer[i] < 127) {
            debug_log << buffer[i];
        } else {
            debug_log << "\\x" << std::hex << (int)(unsigned char)buffer[i] << std::dec;
        }
    }
    debug_log << "\n";
    
    if (bytes > 0) {
        std::string response(buffer, bytes);
        
        // find the FULLRESYNC line
        size_t pos = response.find("\r\n");
        if (pos != std::string::npos) {
            std::string fullresync_line = response.substr(0, pos);
            debug_log << "DEBUG REPLICA: FULLRESYNC line: " << fullresync_line << "\n";
            
            pos += 2; // skip \r\n
            
            // check for RDB file
            if (pos < response.size() && response[pos] == '$') {
                size_t len_end = response.find("\r\n", pos);
                if (len_end != std::string::npos) {
                    int rdb_len = std::stoi(response.substr(pos + 1, len_end - pos - 1));
                    debug_log << "DEBUG REPLICA: RDB file size: " << rdb_len << " bytes\n";
                    
                    // calculate how much of the RDB we've received
                    int rdb_start = len_end + 2;
                    int total_received = bytes - rdb_start;
                    
                    debug_log << "DEBUG REPLICA: Already received " << total_received << " of " << rdb_len << " RDB bytes\n";
                    
                    // receive remaining RDB data if needed
                    while (total_received < rdb_len) {
                        memset(buffer, 0, sizeof(buffer));
                        int to_receive = std::min((int)sizeof(buffer), rdb_len - total_received);
                        int received = recv(master_connection_fd, buffer, to_receive, 0);
                        if (received <= 0) {
                            debug_log << "DEBUG REPLICA: Error receiving RDB, received=" << received << "\n";
                            break;
                        }
                        total_received += received;
                        debug_log << "DEBUG REPLICA: Received additional " << received << " bytes, total: " 
                                  << total_received << "/" << rdb_len << "\n";
                    }
                    debug_log << "DEBUG REPLICA: RDB file fully received\n";
                }
            }
        }
    }
    
    replica_offset = 0;
    debug_log << "DEBUG REPLICA: Handshake completed successfully\n";
    
    // the master should now have this connection registered as a replica
    // and should start sending commands to it
    
    return true;
}


std::string escape_string(const std::string& s) {
    std::stringstream ss;
    for (char c : s) {
        if (c == '\r') ss << "\\r";
        else if (c == '\n') ss << "\\n";
        else if (c < 32 || c > 126) ss << "\\x" << std::hex << (int)(unsigned char)c;
        else ss << c;
    }
    return ss.str();
}


void process_replication_stream() {
    if (master_connection_fd < 0) {
        debug_log << "DEBUG REPLICA STREAM: No master connection\n";
        return;
    }
    
    char buffer[4096];
    std::string pending_data;
    bool rdb_consumed = false;
    
    debug_log << "DEBUG REPLICA STREAM: Starting on fd=" << master_connection_fd << "\n";
    
    while (true) {
        int bytes = recv(master_connection_fd, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            debug_log << "DEBUG REPLICA STREAM: Connection lost (bytes=" << bytes << ")\n";
            std::lock_guard<std::mutex> lock(master_conn_mutex);
            close(master_connection_fd);
            master_connection_fd = -1;
            return;
        }
        
        debug_log << "DEBUG REPLICA STREAM: Received " << bytes << " bytes\n";
        
        // check if this is the RDB file
        if (!rdb_consumed && bytes > 0 && buffer[0] == '$') {
            debug_log << "DEBUG REPLICA STREAM: Detected RDB file\n";
            
            // parse the RDB bulk string
            std::string temp(buffer, bytes);
            size_t crlf = temp.find("\r\n");
            if (crlf != std::string::npos) {
                int rdb_size = std::stoi(temp.substr(1, crlf - 1));
                int total_rdb_bytes = crlf + 2 + rdb_size;
                
                debug_log << "DEBUG REPLICA STREAM: RDB size=" << rdb_size 
                          << ", total bytes to skip=" << total_rdb_bytes << "\n";
                
                if (bytes >= total_rdb_bytes) {
                    pending_data.append(buffer + total_rdb_bytes, bytes - total_rdb_bytes);
                    rdb_consumed = true;
                    debug_log << "DEBUG REPLICA STREAM: RDB consumed, " 
                              << (bytes - total_rdb_bytes) << " bytes remain\n";
                } else {
                    debug_log << "DEBUG REPLICA STREAM: Partial RDB, skipping\n";
                    continue;
                }
            }
        } else {
            pending_data.append(buffer, bytes);
            debug_log << "DEBUG REPLICA STREAM: Appended to pending, total=" 
                      << pending_data.size() << " bytes\n";
        }
        
        // try to parse commands
        while (true) {
            size_t size_before = pending_data.size();
            auto command = parse_array_command(pending_data);
            if (command.empty()) {
                debug_log << "DEBUG REPLICA STREAM: No complete command (pending=" 
                          << pending_data.size() << " bytes)\n";
                break;
            }
            
            size_t bytes_consumed = size_before - pending_data.size();
            
            if (command.size() > 0) {
                std::string cmd = command[0];
                std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
                
                debug_log << "DEBUG REPLICA STREAM: Parsed command: " << cmd;
                for (size_t i = 1; i < command.size(); i++) {
                    debug_log << " " << command[i];
                }
                debug_log << "\n";
                
                // handle commands
                if (cmd == "SET") {
                    debug_log << "DEBUG REPLICA STREAM: Processing SET\n";
                    std::string result = handle_set(command);
                    replica_offset += bytes_consumed;
                }
                else if (cmd == "REPLCONF" && command.size() >= 3) {
                    std::string subcmd = command[1];
                    std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(), ::toupper);
                    
                    debug_log << "DEBUG REPLICA STREAM: REPLCONF subcmd=" << subcmd << "\n";
                    
                    if (subcmd == "GETACK") {
                        long long current_offset = replica_offset.load();
                        debug_log << "DEBUG REPLICA STREAM: GETACK received, offset=" 
                                  << current_offset << ", sending ACK\n";
                        
                        std::vector<std::string> ack_response = {"REPLCONF", "ACK", std::to_string(current_offset)};
                        std::string ack_resp = encode_as_resp_array(ack_response);
                        
                        int sent = send(master_connection_fd, ack_resp.c_str(), ack_resp.size(), 0);
                        debug_log << "DEBUG REPLICA STREAM: Sent ACK, bytes=" << sent << "\n";
                        
                        replica_offset += bytes_consumed;
                    }
                }
                else if (cmd == "PING") {
                    debug_log << "DEBUG REPLICA STREAM: Processing PING\n";
                    replica_offset += bytes_consumed;
                }
                else {
                    debug_log << "DEBUG REPLICA STREAM: Other command, consuming " 
                              << bytes_consumed << " bytes\n";
                    replica_offset += bytes_consumed;
                }
            }
        }
    }
}