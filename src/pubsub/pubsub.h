#ifndef PUBSUB_H
#define PUBSUB_H

#include "../server.h"

// track which clients are subscribed to correspondnng channel
struct SubscriptionInfo {
    std::set<std::string> subscribed_channels;
    bool in_subscribe_mode = false;
};

// global subscription management
extern std::mutex pubsub_mutex;
extern std::map<std::string, std::set<int>> channel_subscribers;  // channel -> set of client fds
extern std::map<int, SubscriptionInfo> client_subscriptions;      // client fd -> subscription info

// command handlers
std::string handle_subscribe(const std::vector<std::string>& args, int client_fd);
std::string handle_publish(const std::vector<std::string>& args);

// helpers
bool is_client_in_subscribe_mode(int client_fd);
void cleanup_subscriber(int client_fd);

#endif