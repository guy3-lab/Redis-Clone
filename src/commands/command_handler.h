#ifndef COMMAND_HANDLER_H
#define COMMAND_HANDLER_H

#include "../server.h"

// transaction structure
struct TransactionState {
    bool in_transaction;
    std::vector<std::vector<std::string>> queued_commands;
};

std::string execute_command(const std::vector<std::string>& message, int client_fd, bool from_replication = false);
void handle_client(int client_fd);
bool is_in_transaction(int client_fd);

#endif