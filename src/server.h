#ifndef SERVER_H
#define SERVER_H

#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <deque>
#include <condition_variable>
#include <set>
#include <atomic>
#include <map>
#include <sstream>
#include <fstream>
#include <random>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <memory>

// debug tracing, silent unless REDIS_DEBUG=1 (defined in main.cpp)
extern std::ostream debug_log;

#endif