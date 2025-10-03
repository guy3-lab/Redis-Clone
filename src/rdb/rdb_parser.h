#ifndef RDB_PARSER_H
#define RDB_PARSER_H

#include "../server.h"

struct RDBConfig {
    std::string dir;
    std::string dbfilename;
};

extern RDBConfig rdb_config;

// rdb parsing functions
bool load_rdb_file();
std::pair<std::string, long long> parse_string_encoding(const unsigned char* data, size_t& pos, size_t max_size);
long long parse_length_encoding(const unsigned char* data, size_t& pos, size_t max_size);

// command handlers
std::string handle_config_get(const std::vector<std::string>& args);
std::string handle_keys(const std::vector<std::string>& args);

#endif