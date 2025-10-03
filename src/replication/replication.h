#ifndef REPLICATION_H
#define REPLICATION_H

#include "../server.h"

struct ReplicationConfig {
    bool is_replica = false;
    std::string master_host;
    int master_port;
    int listening_port = 6379;
    std::string replication_id;
    std::atomic<long long> replication_offset{0};
    std::atomic<long long> last_write_offset{0};
};

struct ConnectedReplica {
    int fd;
    std::string listening_port;
    std::atomic<long long> ack_offset{0};
    std::atomic<bool> ack_received{false};
    long long last_ping_time = 0;
    bool active = true;
    std::thread response_thread;
    long long connection_offset = 0;
};

extern ReplicationConfig repl_config;
extern std::mutex replicas_mutex;
extern std::map<int, std::unique_ptr<ConnectedReplica>> connected_replicas;
extern int master_connection_fd;
extern std::mutex master_conn_mutex;
extern std::atomic<long long> replica_offset;

// master functions
std::string handle_psync(int client_fd, const std::vector<std::string>& message);
std::string handle_wait(const std::vector<std::string>& message);
std::string handle_replconf(const std::vector<std::string>& message, bool is_replica);
std::string handle_info_replication();
void propagate_to_replicas(const std::vector<std::string>& command);
void process_replica_responses(int replica_fd);

// replica functions
bool connect_to_master();
void process_replication_stream();

#endif