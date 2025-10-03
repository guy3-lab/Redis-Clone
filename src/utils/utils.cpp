#include "utils.h"

long long get_current_time_ms() {
    auto now = std::chrono::system_clock::now();
    auto time_since_beginning = now.time_since_epoch();
    auto time_since_beginning_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_since_beginning);
    return time_since_beginning_ms.count();
}

std::string generate_random_string(int length) {
    const char charset[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, sizeof(charset) - 2);
    
    std::string result;
    for (int i = 0; i < length; i++) {
        result += charset[dis(gen)];
    }
    return result;
}

bool is_valid_integer(const std::string& str, long long& result) {
    if (str.empty()) return false;
    
    try {
        size_t pos;
        result = std::stoll(str, &pos);
        return pos == str.length();
    } catch (...) {
        return false;
    }
}