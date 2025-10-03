#ifndef UTILS_H
#define UTILS_H

#include "../server.h"

long long get_current_time_ms();
std::string generate_random_string(int length);
bool is_valid_integer(const std::string& str, long long& result);

#endif