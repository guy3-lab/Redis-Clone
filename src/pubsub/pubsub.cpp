#include "pubsub.h"
#include "../protocol/resp_parser.h"

std::mutex pubsub_mutex;
std::map<std::string, std::set<int>> channel_subscribers;
std::map<int, SubscriptionInfo> client_subscriptions;

bool is_client_in_subscribe_mode(int client_fd) {
    std::lock_guard<std::mutex> lock(pubsub_mutex);
    return client_subscriptions.count(client_fd) > 0 && 
           client_subscriptions[client_fd].in_subscribe_mode;
}

void cleanup_subscriber(int client_fd) {
    std::lock_guard<std::mutex> lock(pubsub_mutex);
    
    std::cerr << "PUBSUB: Cleaning up subscriber fd=" << client_fd << std::endl;
    
    // remove this client from all channels its subscribed to
    if (client_subscriptions.count(client_fd)) {
        for (const auto& channel : client_subscriptions[client_fd].subscribed_channels) {
            if (channel_subscribers.count(channel)) {
                channel_subscribers[channel].erase(client_fd);
                if (channel_subscribers[channel].empty()) {
                    channel_subscribers.erase(channel);
                }
            }
        }
        client_subscriptions.erase(client_fd);
    }
}

std::string handle_subscribe(const std::vector<std::string>& args, int client_fd) {
    if (args.size() < 2) {
        return encode_error("ERR wrong number of arguments for 'subscribe' command");
    }
    
    std::string response;
    
    std::lock_guard<std::mutex> lock(pubsub_mutex);
    
    // mark client as being in subscribe mode
    client_subscriptions[client_fd].in_subscribe_mode = true;
    
    // subscribe to each channel provided
    for (size_t i = 1; i < args.size(); i++) {
        std::string channel = args[i];
        
        // add channel to this client's subscription list
        client_subscriptions[client_fd].subscribed_channels.insert(channel);
        
        // add this client to the channel's subscriber list
        channel_subscribers[channel].insert(client_fd);
        
        // get current subscription count for this client
        int subscription_count = client_subscriptions[client_fd].subscribed_channels.size();
        
        // build subscribe confirmation response
        // ex: *3\r\n$9\r\nsubscribe\r\n$<len>\r\n<channel>\r\n:<count>\r\n
        response += "*3\r\n";
        response += encode_bulk_string("subscribe");
        response += encode_bulk_string(channel);
        response += encode_integer(subscription_count);
        
        std::cerr << "PUBSUB: Client " << client_fd << " subscribed to '" << channel 
                  << "', now has " << subscription_count << " subscriptions" << std::endl;
    }
    
    return response;
}

std::string handle_publish(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'publish' command");
    }
    
    std::string channel = args[1];
    std::string message = args[2];
    
    int subscriber_count = 0;
    std::vector<int> subscribers_to_notify;
    
    {
        std::lock_guard<std::mutex> lock(pubsub_mutex);
        
        // find all subscribers to this channel
        if (channel_subscribers.count(channel)) {
            subscribers_to_notify.assign(
                channel_subscribers[channel].begin(),
                channel_subscribers[channel].end()
            );
            subscriber_count = subscribers_to_notify.size();
        }
    }
    
    std::cerr << "PUBSUB: Publishing to channel '" << channel 
              << "' with " << subscriber_count << " subscribers" << std::endl;
    
    // send message to all subscribers
    // ex: *3\r\n$7\r\nmessage\r\n$<channel_len>\r\n<channel>\r\n$<msg_len>\r\n<message>\r\n
    if (subscriber_count > 0) {
        std::string notification = "*3\r\n";
        notification += encode_bulk_string("message");
        notification += encode_bulk_string(channel);
        notification += encode_bulk_string(message);
        
        for (int subscriber_fd : subscribers_to_notify) {
            int sent = send(subscriber_fd, notification.c_str(), notification.size(), MSG_NOSIGNAL);
            if (sent < 0) {
                std::cerr << "PUBSUB: Failed to send to subscriber fd=" << subscriber_fd << std::endl;
            } else {
                std::cerr << "PUBSUB: Sent message to subscriber fd=" << subscriber_fd << std::endl;
            }
        }
    }
    
    // return num clients that received message
    return encode_integer(subscriber_count);
}