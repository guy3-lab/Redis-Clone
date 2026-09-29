#include "server.h"
#include "utils/utils.h"
#include "protocol/resp_parser.h"
#include "commands/command_handler.h"
#include "replication/replication.h"
#include "blocking/blocking.h"
#include "rdb/rdb_parser.h"

// global state
ReplicationConfig repl_config;

// transaction state
std::mutex transaction_mutex;
std::unordered_map<int, TransactionState> client_transactions;

// no buffer attached = writes are dropped almost for free
// synchronous logging on every command was capping throughput at ~17k ops/sec
std::ostream debug_log(nullptr);

int main(int argc, char **argv) {
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    const char* debug_env = std::getenv("REDIS_DEBUG");
    if (debug_env && std::string(debug_env) == "1") {
        debug_log.rdbuf(std::cerr.rdbuf());
        debug_log << std::unitbuf;
    }

    debug_log << "DEBUG MAIN: Starting with " << argc << " arguments\n";
    for (int i = 0; i < argc; i++) {
        debug_log << "  arg[" << i << "]: " << argv[i] << "\n";
    }
    
    // parse command line arguments
    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        
        if (arg == "--port" && i + 1 < argc) {
            repl_config.listening_port = std::stoi(argv[++i]);
            debug_log << "DEBUG MAIN: Set port to " << repl_config.listening_port << "\n";
        } else if (arg == "--replicaof" && i + 1 < argc) {
            std::string master_info = argv[++i];
            debug_log << "DEBUG MAIN: Got --replicaof with: " << master_info << "\n";
            size_t space_pos = master_info.find(' ');
            if (space_pos != std::string::npos) {
                repl_config.master_host = master_info.substr(0, space_pos);
                repl_config.master_port = std::stoi(master_info.substr(space_pos + 1));
                repl_config.is_replica = true;
                debug_log << "DEBUG MAIN: Configured as replica of " 
                          << repl_config.master_host << ":" << repl_config.master_port << "\n";
            } else {
                debug_log << "DEBUG MAIN: ERROR - Invalid replicaof format, no space found\n";
            }
        } else if (arg == "--dir" && i + 1 < argc) {
            rdb_config.dir = argv[++i];
            debug_log << "DEBUG MAIN: Set dir to " << rdb_config.dir << "\n";
        } else if (arg == "--dbfilename" && i + 1 < argc) {
            rdb_config.dbfilename = argv[++i];
            debug_log << "DEBUG MAIN: Set dbfilename to " << rdb_config.dbfilename << "\n";
        }
    }
    
    debug_log << "DEBUG MAIN: is_replica = " << repl_config.is_replica << "\n";
    
    // load RDB file if coonfiged
    if (!rdb_config.dir.empty() && !rdb_config.dbfilename.empty()) {
        debug_log << "DEBUG MAIN: Loading RDB file from " << rdb_config.dir << "/" << rdb_config.dbfilename << "\n";
        if (!load_rdb_file()) {
            std::cerr << "WARNING: Failed to load RDB file, starting with empty database\n";
        }
    } else {
        debug_log << "DEBUG MAIN: No RDB configuration, starting with empty database\n";
    }
    
    // setup server socket FIRST
    // HAS to come before any replication logic
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }
    
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(repl_config.listening_port);
    
    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port " << repl_config.listening_port << "\n";
        return 1;
    }
    
    if (listen(server_fd, 5) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }
    
    std::cout << "Redis server started on port " << repl_config.listening_port;
    if (repl_config.is_replica) {
        std::cout << " as replica of " << repl_config.master_host << ":" << repl_config.master_port;
    } else {
        std::cout << " as master";
    }
    std::cout << std::endl;
    
    // NOW initialize replication after server is listening
    if (!repl_config.is_replica) {
        repl_config.replication_id = generate_random_string(40);
        repl_config.replication_offset = 0;
        repl_config.last_write_offset = 0;
        debug_log << "DEBUG MAIN: Initialized as master with ID " << repl_config.replication_id << "\n";
    } else {
        // start replica connection in background thread
        debug_log << "DEBUG MAIN: Starting replica connection thread\n";
        std::thread replica_thread([]() {
            debug_log << "DEBUG REPLICA THREAD: Thread started, will connect to " 
                      << repl_config.master_host << ":" << repl_config.master_port << "\n";
            
            // small delay to make sure server is ready
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            
            int attempts = 0;
            while (true) {
                debug_log << "DEBUG REPLICA THREAD: Connection attempt " << ++attempts << "\n";
                if (connect_to_master()) {
                    debug_log << "DEBUG REPLICA THREAD: Connected successfully, starting stream processing\n";
                    process_replication_stream();
                    debug_log << "DEBUG REPLICA THREAD: Stream processing ended, will reconnect\n";
                } else {
                    debug_log << "DEBUG REPLICA THREAD: Connection failed, waiting 1s before retry\n";
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        });
        replica_thread.detach();
        debug_log << "DEBUG MAIN: Replica thread detached\n";
    }
    
    // start background threads
    std::thread blocked_processor(process_blocked_clients);
    blocked_processor.detach();
    
    std::thread xread_processor(process_xread_blocked_clients);
    xread_processor.detach();
    
    // accept connections
    while (true) {
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
        if (client_fd < 0) {
            std::cerr << "Failed to accept client connection\n";
            continue;
        }
        
        std::thread client_thread(handle_client, client_fd);
        client_thread.detach();
    }
    
    close(server_fd);
    return 0;
}