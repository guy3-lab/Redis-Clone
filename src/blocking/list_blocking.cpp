#include "blocking.h"
#include "../protocol/resp_parser.h"
#include "../storage/storage.h"
#include "../utils/utils.h"

std::list<BlockedClient*> blocked_clients;

// caller holds storage_mutex
static bool pop_first_available(const std::vector<std::string>& keys, const std::string& command,
                                std::string& key_out, std::string& value_out) {
    for (const auto& key : keys) {
        auto it = listStorage.find(key);
        if (it == listStorage.end() || it->second.empty()) continue;

        auto& list = it->second;
        if (command == "BLPOP") {
            value_out = list.front();
            list.pop_front();
        } else { // BRPOP
            value_out = list.back();
            list.pop_back();
        }

        if (list.empty()) {
            listStorage.erase(it);
        }

        key_out = key;
        return true;
    }
    return false;
}

static std::string encode_pop_reply(const std::string& key, const std::string& value) {
    std::string response = "*2\r\n";
    response += encode_bulk_string(key);
    response += encode_bulk_string(value);
    return response;
}

// called after LPUSH/RPUSH, hands elements to waiters in the order they blocked
void notify_blocked_clients() {
    std::lock_guard<std::mutex> lock(storage_mutex);

    for (auto it = blocked_clients.begin(); it != blocked_clients.end();) {
        BlockedClient* waiter = *it;
        if (pop_first_available(waiter->keys, waiter->command_type, waiter->key, waiter->value)) {
            waiter->served = true;
            waiter->cv.notify_one();
            it = blocked_clients.erase(it);
        } else {
            ++it;
        }
    }
}

std::string handle_blocking_pop(const std::vector<std::string>& args, const std::string& command) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for '" + command + "' command");
    }

    // parse timeout (last argument), 0 means wait forever
    double timeout_seconds = std::stod(args[args.size() - 1]);
    long long timeout_ms = (timeout_seconds == 0) ? 0 : (long long)(timeout_seconds * 1000);

    // extract keys (all arguments except command and timeout)
    std::vector<std::string> keys(args.begin() + 1, args.end() - 1);

    std::unique_lock<std::mutex> lock(storage_mutex);

    std::string popped_key;
    std::string popped_value;
    if (pop_first_available(keys, command, popped_key, popped_value)) {
        return encode_pop_reply(popped_key, popped_value);
    }

    // redis doesn't block inside a transaction
    // and EXEC holds transaction_mutex, so waiting here would stall every client
    if (in_exec) {
        return "$-1\r\n";
    }

    // park this thread until a pusher serves it or the timeout hits
    // waiting releases storage_mutex, so pushers can get in
    BlockedClient waiter;
    waiter.keys = keys;
    waiter.command_type = command;
    blocked_clients.push_back(&waiter);

    auto served = [&waiter] { return waiter.served; };
    if (timeout_ms == 0) {
        waiter.cv.wait(lock, served);
    } else if (!waiter.cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), served)) {
        blocked_clients.remove(&waiter);
        return "$-1\r\n";
    }

    // notify_blocked_clients already removed us from the list
    return encode_pop_reply(waiter.key, waiter.value);
}
