#include "resp_parser.h"

std::vector<std::string> parse_array_command(std::string& respString) { 
    int index = 0;
    std::vector<std::string> result;

    if(respString.empty() || respString[index] != '*') return result;
    index++;
    
    int array_length = 0;
    while(index < respString.length() && respString[index] != '\r'){
        array_length = array_length * 10 + (respString[index] - '0');
        index++;
    }
    if(index >= respString.length()) return result;
    index+=2;
    
    for(int i = 0; i < array_length; i++){
        if(index >= respString.length() || respString[index] != '$') return result;
        index++;

        int str_length = 0;
        while(index < respString.length() && respString[index] != '\r'){
            str_length = str_length * 10 + (respString[index] - '0');
            index++;
        }
        if(index >= respString.length()) return result;
        index+=2;

        if(index + str_length > respString.length()) return result;
        
        std::string message = respString.substr(index, str_length);
        index += str_length;
        
        if(index + 2 > respString.length()) return result;
        index+=2;
        result.push_back(message);
    }

    respString.erase(0, index);
    return result;
}

std::string encode_as_resp_array(const std::vector<std::string>& command) {
    std::string encoded = "*" + std::to_string(command.size()) + "\r\n";
    for (const auto& arg : command) {
        encoded += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
    }
    return encoded;
}

std::string encode_bulk_string(const std::string& str) {
    if (str.empty()) {
        return "$-1\r\n";
    }
    return "$" + std::to_string(str.size()) + "\r\n" + str + "\r\n";
}

std::string encode_simple_string(const std::string& str) {
    return "+" + str + "\r\n";
}

std::string encode_error(const std::string& error) {
    return "-" + error + "\r\n";
}

std::string encode_integer(long long value) {
    return ":" + std::to_string(value) + "\r\n";
}