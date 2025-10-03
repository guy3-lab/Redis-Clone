#ifndef SORTED_SETS_H
#define SORTED_SETS_H

#include "../server.h"

// sorted set member with score and name
struct SortedSetMember {
    double score;
    std::string member;
    
    // comparator for ordering
    // by score first then lexicographically
    bool operator<(const SortedSetMember& other) const {
        if (score != other.score) {
            return score < other.score;
        }
        return member < other.member;
    }
};

// ses a set for sorted access and a map for O(1) lookups
struct SortedSet {
    std::set<SortedSetMember> sorted_members;  // for range queries
    std::map<std::string, double> member_scores;  // for quick score lookup
};

// global sorted set storage
extern std::unordered_map<std::string, SortedSet> sortedSetStorage;

// command handlers
std::string handle_zadd(const std::vector<std::string>& args);
std::string handle_zrank(const std::vector<std::string>& args);
std::string handle_zrange(const std::vector<std::string>& args);
std::string handle_zcard(const std::vector<std::string>& args);
std::string handle_zscore(const std::vector<std::string>& args);
std::string handle_zrem(const std::vector<std::string>& args);

#endif