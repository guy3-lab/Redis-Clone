#include "blocking.h"

std::condition_variable xread_cv;

// called after XADD, reads don't consume entries so every waiter can just recheck
void notify_xread_blocked_clients() {
    xread_cv.notify_all();
}
