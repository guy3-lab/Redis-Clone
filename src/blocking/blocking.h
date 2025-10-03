#ifndef BLOCKING_H
#define BLOCKING_H

#include "../server.h"

struct BlockedClient {
    int client_fd;
    std::vector<std::string> keys;
    long long timeout_ms;
    long long start_time_ms;
    std::string command_type;
};

struct XReadBlockedClient {
    int client_fd;
    std::vector<std::string> stream_keys;
    std::vector<std::string> stream_ids;
    long long timeout_ms;
    long long start_time_ms;
};

extern std::mutex blocked_clients_mutex;
extern std::vector<BlockedClient> blocked_clients;
extern std::mutex xread_blocked_mutex;
extern std::vector<XReadBlockedClient> xread_blocked_clients;
extern std::mutex blocked_fds_mutex;
extern std::set<int> blocked_fds;

std::string handle_blocking_pop(const std::vector<std::string>& args, int client_fd, const std::string& command);
void process_blocked_clients();
void process_xread_blocked_clients();
bool is_client_blocked(int client_fd);
void notify_blocked_clients();
void notify_xread_blocked_clients();
void cleanup_client(int client_fd);

#endif