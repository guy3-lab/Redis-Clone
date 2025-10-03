#ifndef RESP_PARSER_H
#define RESP_PARSER_H

#include "../server.h"

std::vector<std::string> parse_array_command(std::string& respString);
std::string encode_as_resp_array(const std::vector<std::string>& command);
std::string encode_bulk_string(const std::string& str);
std::string encode_simple_string(const std::string& str);
std::string encode_error(const std::string& error);
std::string encode_integer(long long value);

#endif 