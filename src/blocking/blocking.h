#ifndef BLOCKING_H
#define BLOCKING_H

#include "../server.h"
#include <list>

// a client parked in BLPOP/BRPOP
// the pushing thread pops an element for it, fills in key/value, and wakes it
struct BlockedClient {
    std::vector<std::string> keys;
    std::string command_type;
    std::condition_variable cv;
    bool served = false;
    std::string key;
    std::string value;
};

// guarded by storage_mutex, oldest waiter first
extern std::list<BlockedClient*> blocked_clients;

// XREAD BLOCK waiters recheck their streams whenever this fires (guarded by storage_mutex)
extern std::condition_variable xread_cv;

// true while EXEC runs queued commands, blocking commands return right away (defined in command_handler.cpp)
extern thread_local bool in_exec;

std::string handle_blocking_pop(const std::vector<std::string>& args, const std::string& command);
void notify_blocked_clients();
void notify_xread_blocked_clients();

#endif
