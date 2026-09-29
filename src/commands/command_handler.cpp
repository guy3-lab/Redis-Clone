#include "command_handler.h"
#include "../protocol/resp_parser.h"
#include "../storage/storage.h"
#include "../replication/replication.h"
#include "../blocking/blocking.h"
#include "../rdb/rdb_parser.h"
#include "../pubsub/pubsub.h"
#include "../storage/sorted_sets.h"
#include "../geo/geo.h"

// transaction state management
extern std::mutex transaction_mutex;
extern std::unordered_map<int, TransactionState> client_transactions;
std::mutex replica_connections_mutex;
std::set<int> replica_connections;

bool is_in_transaction(int client_fd) {
    std::lock_guard<std::mutex> lock(transaction_mutex);
    return client_transactions.count(client_fd) > 0 && 
           client_transactions[client_fd].in_transaction;
}

std::string execute_command(const std::vector<std::string>& message, int client_fd, bool from_replication) {
    if (message.empty()) {
        return encode_error("ERR empty command");
    }
    
    std::string command = message[0];
    std::transform(command.begin(), command.end(), command.begin(), ::toupper);

    debug_log << "CMD_RECV: fd=" << client_fd << " command=" << command << " from_repl=" << from_replication << "\n";
    
    // basic commands
    if (command == "PING") {
        if (from_replication) return "";
        return encode_simple_string("PONG");
    }
    else if (command == "ECHO") {
        if (message.size() < 2) return encode_error("ERR wrong number of arguments");
        if (from_replication) return "";  // don't respond to replicated commands
        return encode_bulk_string(message[1]);
    }

    // config commands
    else if (command == "CONFIG") {
        if (from_replication) return "";
        if (message.size() < 2) return encode_error("ERR wrong number of arguments");
        
        std::string subcommand = message[1];
        std::transform(subcommand.begin(), subcommand.end(), subcommand.begin(), ::toupper);
        
        if (subcommand == "GET") {
            return handle_config_get(message);
        }
        return encode_error("ERR unknown CONFIG subcommand");
    }
    
    // keys command
    else if (command == "KEYS") {
        if (from_replication) return "";
        return handle_keys(message);
    }
    
    // storage commands
    else if (command == "SET") {
        // process SET even when from_replication
        std::string result = handle_set(message);
        
        // only propagate if this is from a client to a master
        if (!from_replication && !repl_config.is_replica) {
            propagate_to_replicas(message);
        }
        
        // only return response if not from replication
        return from_replication ? "" : result;
    }
    else if (command == "GET") {
        // GET should work normally even for replicas
        return handle_get(message);
    }
    else if (command == "INCR") {
        std::string result = handle_incr(message);
        if (!from_replication && !repl_config.is_replica) {
            propagate_to_replicas(message);
        }
        return from_replication ? "" : result;
    }
    else if (command == "TYPE") {
        return handle_type(message);
    }
    
    // list commands
    else if (command == "LPUSH") {
        std::string result = handle_lpush(message);
        if (!from_replication && !repl_config.is_replica) {
            propagate_to_replicas(message);
            notify_blocked_clients();
        }
        return from_replication ? "" : result;
    }
    else if (command == "RPUSH") {
        std::string result = handle_rpush(message);
        if (!from_replication && !repl_config.is_replica) {
            propagate_to_replicas(message);
            notify_blocked_clients();
        }
        return from_replication ? "" : result;
    }

    else if (command == "LPOP") {
        std::string result = handle_lpop(message);
        if (!from_replication && !repl_config.is_replica && result[0] != '-') {
            propagate_to_replicas(message);
        }
        return from_replication ? "" : result;
    }
    else if (command == "RPOP") {
        std::string result = handle_rpop(message);
        if (!from_replication && !repl_config.is_replica && result[0] != '-') {
            propagate_to_replicas(message);
        }
        return from_replication ? "" : result;
    }
    else if (command == "LRANGE") {
        return handle_lrange(message);
    }
    else if (command == "LLEN") {
        return handle_llen(message);
    }
    else if (command == "BLPOP" || command == "BRPOP") {
        return handle_blocking_pop(message, client_fd, command);
    }
    
    // stream commands
    else if (command == "XADD") {
        std::string result = handle_xadd(message);
        if (!from_replication && !repl_config.is_replica && result[0] == '$') {
            propagate_to_replicas(message);
            notify_xread_blocked_clients();
        }
        return from_replication ? "" : result;
    }
    else if (command == "XRANGE") {
        return handle_xrange(message);
    }
    else if (command == "XREAD") {
        return handle_xread(message, client_fd);
    }

    // sorted set commands
    else if (command == "ZADD") {
        std::string result = handle_zadd(message);
        if (!from_replication && !repl_config.is_replica && result[0] == ':') {
            propagate_to_replicas(message);
        }
        return from_replication ? "" : result;
    }
    else if (command == "ZRANK") {
        return handle_zrank(message);
    }
    else if (command == "ZRANGE") {
        return handle_zrange(message);
    }
    else if (command == "ZCARD") {
        return handle_zcard(message);
    }
    else if (command == "ZSCORE") {
        return handle_zscore(message);
    }
    else if (command == "ZREM") {
        std::string result = handle_zrem(message);
        if (!from_replication && !repl_config.is_replica && result[0] == ':') {
            propagate_to_replicas(message);
        }
        return from_replication ? "" : result;
    }

    // geo commands
    else if (command == "GEOADD") {
        std::string result = handle_geoadd(message);
        if (!from_replication && !repl_config.is_replica && result[0] == ':') {
            propagate_to_replicas(message);
        }
        return from_replication ? "" : result;
    }
    else if (command == "GEOPOS") {
        return handle_geopos(message);
    }
    else if (command == "GEODIST") {
        return handle_geodist(message);
    }
    else if (command == "GEOSEARCH") {
        return handle_geosearch(message);
    }
    
    // replication commands
    else if (command == "INFO") {
        if (from_replication) return "";  // don't process INFO from replication
        if (message.size() < 2) return encode_error("ERR wrong number of arguments");
        std::string section = message[1];
        std::transform(section.begin(), section.end(), section.begin(), ::tolower);
        if (section == "replication") {
            return handle_info_replication();
        }
        return encode_error("ERR unknown section");
    }
    else if (command == "REPLCONF") {
        if (from_replication) return "";  // handle REPLCONF in replication stream
        return handle_replconf(message, repl_config.is_replica);
    }
    else if (command == "PSYNC") {
        if (from_replication) return "";
        return handle_psync(client_fd, message);
    }
    else if (command == "WAIT") {
        if (from_replication) return "";
        return handle_wait(message);
    }

    // pubnsub commands
    else if (command == "SUBSCRIBE") {
        return handle_subscribe(message, client_fd);
    }
    else if (command == "PUBLISH") {
        if (from_replication) return "";
        return handle_publish(message);
    }

    // error
    else {
        return encode_error("ERR unknown command '" + message[0] + "'");
    }
}



void handle_client(int client_fd) {
    char buffer[4096];
    std::string pending_data;
    bool is_replica_connection = false;
    long long replica_bytes_processed = 0;
    
    while (true) {
        // DON'T EXIT for replica connections anymore
        // need to keep reading to get ACK responses
        
        if (!is_replica_connection && is_client_blocked(client_fd)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        
        memset(buffer, 0, sizeof(buffer));
        int bytes_read = read(client_fd, buffer, sizeof(buffer));
        
        if (bytes_read <= 0) {
            cleanup_client(client_fd);
            cleanup_subscriber(client_fd); 
            
            // clean up transaction state
            {
                std::lock_guard<std::mutex> lock(transaction_mutex);
                client_transactions.erase(client_fd);
            }
            
            // remove from replica connections if it was one
            {
                std::lock_guard<std::mutex> lock(replica_connections_mutex);
                replica_connections.erase(client_fd);
            }

            // clean up replica state if this was a replica
            {
                std::lock_guard<std::mutex> lock(replicas_mutex);
                if (connected_replicas.count(client_fd)) {
                    debug_log << "DEBUG: Cleaning up replica fd=" << client_fd << std::endl;
                    connected_replicas[client_fd]->active = false;
                    if (connected_replicas[client_fd]->response_thread.joinable()) {
                        connected_replicas[client_fd]->response_thread.detach();
                    }
                    connected_replicas.erase(client_fd);
                }
            }
            
            break;
        }
        
        pending_data.append(buffer, bytes_read);
        
        while (true) {
            size_t size_before = pending_data.size();
            auto message = parse_array_command(pending_data);
            if (message.empty()) break;
            
            size_t bytes_consumed = size_before - pending_data.size();
            
            std::string command = message[0];
            std::transform(command.begin(), command.end(), command.begin(), ::toupper);

            debug_log << "CLIENT_CMD: fd=" << client_fd << " parsed command=" << command 
              << " is_replica_conn=" << is_replica_connection << "\n";

            // check if client is in subscribe mode
            bool in_subscribe_mode = is_client_in_subscribe_mode(client_fd);
            
            if (in_subscribe_mode) {
                // now in subscribe mode
                // only certain commands are allowed
                if (command != "SUBSCRIBE" && command != "UNSUBSCRIBE" && 
                    command != "PSUBSCRIBE" && command != "PUNSUBSCRIBE" && 
                    command != "PING" && command != "QUIT") {
                    std::string response = encode_error("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
                    send(client_fd, response.c_str(), response.size(), 0);
                    continue;
                }
            }
            
            // if we're a replica connection (after PSYNC), handle responses lil diffy
            if (is_replica_connection) {
                debug_log << "DEBUG REPLICA MODE: Received response: " << command;
                for (size_t i = 1; i < message.size(); i++) {
                    debug_log << " " << message[i];
                }
                debug_log << std::endl;
                
                // handle REPLCONF responses (ACKs from replica)
                if (command == "REPLCONF" && message.size() >= 3) {
                    std::string subcmd = message[1];
                    std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(), ::toupper);
                    
                    if (subcmd == "ACK") {
                        // this is an ACK from the replica
                        long long ack_offset = std::stoll(message[2]);
                        debug_log << "ACK_RECV: fd=" << client_fd << " offset=" << ack_offset << std::endl;
                        
                        // update the replica's ACK state
                        {
                            std::lock_guard<std::mutex> lock(replicas_mutex);
                            if (connected_replicas.count(client_fd)) {
                                connected_replicas[client_fd]->ack_offset = ack_offset;
                                connected_replicas[client_fd]->ack_received = true;
                                debug_log << "ACK_UPDATE: fd=" << client_fd << " updated" << std::endl;
                            } else {
                                debug_log << "ACK_ERROR: fd=" << client_fd << " not in connected_replicas!" << std::endl;
                            }
                        }
                    }
                }
                else if (command == "PING") {
                    // process PING silently
                    replica_bytes_processed += bytes_consumed;
                }
                else if (command == "SET" || command == "DEL" || command == "INCR") {
                    // process write commands silently
                    if (command == "SET" && message.size() >= 3) {
                        std::lock_guard<std::mutex> lock(storage_mutex);
                        storageMap[message[1]] = {message[2], 0};
                        debug_log << "DEBUG REPLICA MODE: Set key=" << message[1] << " value=" << message[2] << std::endl;
                    }
                    replica_bytes_processed += bytes_consumed;
                }
                else {
                    // other commands
                    replica_bytes_processed += bytes_consumed;
                }
                
                // don't send responses for most commands in replica mode
                continue;
            }
            
            std::string response;
            
            // normal client mode (before PSYNC)
            if (command == "PSYNC") {
                response = handle_psync(client_fd, message);
                // handle_psync already sends FULLRESYNC and RDB
                
                // mark as replica connection BUT CONTINUE handling
                is_replica_connection = true;
                debug_log << "FD_TRACK: Marked fd=" << client_fd << " as replica connection" << std::endl;
                {
                    std::lock_guard<std::mutex> lock(replicas_mutex);
                    if (connected_replicas.count(client_fd)) {
                        debug_log << "FD_TRACK: fd=" << client_fd << " IS in connected_replicas" << std::endl;
                    } else {
                        debug_log << "FD_TRACK: WARNING fd=" << client_fd << " NOT in connected_replicas!" << std::endl;
                    }
                }
                replica_bytes_processed = 0;
                debug_log << "DEBUG: Client " << client_fd << " entered replica mode, continuing to handle responses" << std::endl;
                
                // don't exit continue handling in replica mode
                continue;
            }
            
            // check if we're in a transaction
            bool in_transaction = is_in_transaction(client_fd);
            
            if (command == "MULTI") {
                std::lock_guard<std::mutex> lock(transaction_mutex);
                client_transactions[client_fd] = {true, {}};
                response = encode_simple_string("OK");
            }
            else if (command == "EXEC") {
                std::lock_guard<std::mutex> lock(transaction_mutex);
                
                if (!client_transactions.count(client_fd) || 
                    !client_transactions[client_fd].in_transaction) {
                    response = encode_error("ERR EXEC without MULTI");
                } else {
                    auto& state = client_transactions[client_fd];
                    std::vector<std::string> responses;
                    
                    for (const auto& queued_cmd : state.queued_commands) {
                        std::string cmd_response = execute_command(queued_cmd, client_fd);
                        responses.push_back(cmd_response);
                    }
                    
                    response = "*" + std::to_string(responses.size()) + "\r\n";
                    for (const auto& resp : responses) {
                        response += resp;
                    }
                    
                    client_transactions.erase(client_fd);
                }
            }
            else if (command == "DISCARD") {
                std::lock_guard<std::mutex> lock(transaction_mutex);
                
                if (!client_transactions.count(client_fd) || 
                    !client_transactions[client_fd].in_transaction) {
                    response = encode_error("ERR DISCARD without MULTI");
                } else {
                    client_transactions.erase(client_fd);
                    response = encode_simple_string("OK");
                }
            }
            else if (in_transaction) {
                std::lock_guard<std::mutex> lock(transaction_mutex);
                client_transactions[client_fd].queued_commands.push_back(message);
                response = encode_simple_string("QUEUED");
            }
            else {
                response = execute_command(message, client_fd);
            }
            
            if (!response.empty()) {
                send(client_fd, response.c_str(), response.size(), 0);
            }
        }
    }
    
    close(client_fd);
}
