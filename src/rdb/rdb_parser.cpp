#include "rdb_parser.h"
#include "../storage/storage.h"
#include "../utils/utils.h"

RDBConfig rdb_config;

// parse length encoding from RDB file
long long parse_length_encoding(const unsigned char* data, size_t& pos, size_t max_size) {
    if (pos >= max_size) return -1;
    
    unsigned char first_byte = data[pos++];
    unsigned char type = (first_byte & 0xC0) >> 6;  // firstt 2 bits
    
    if (type == 0) {
        // 00: length is the remaining 6 bits
        return first_byte & 0x3F;
    } else if (type == 1) {
        // 01: length is next 14 bits
        if (pos >= max_size) return -1;
        unsigned char next_byte = data[pos++];
        return ((first_byte & 0x3F) << 8) | next_byte;
    } else if (type == 2) {
        // 10: ignore remaining 6 bits, length is next 4 bytes big endian
        if (pos + 4 > max_size) return -1;
        long long length = 0;
        for (int i = 0; i < 4; i++) {
            length = (length << 8) | data[pos++];
        }
        return length;
    } else {
        // 11: special encoding (string as integer)
        return -1;  // will be handled specially
    }
}

// parse string encoding from RDB file
// returns  a pair of (string_value, expiry_ms) 
// expiry_ms is -1 for special formats
std::pair<std::string, long long> parse_string_encoding(const unsigned char* data, size_t& pos, size_t max_size) {
    if (pos >= max_size) return {"", -1};
    
    unsigned char first_byte = data[pos];
    unsigned char type = (first_byte & 0xC0) >> 6;
    
    if (type == 3) {
        // special encoding
        pos++;
        unsigned char format = first_byte & 0x3F;
        
        if (format == 0) {
            // 8-bit integer
            if (pos >= max_size) return {"", -1};
            unsigned char val = data[pos++];
            return {std::to_string(val), -1};
        } else if (format == 1) {
            // 16-bit integer little endian
            if (pos + 2 > max_size) return {"", -1};
            uint16_t val = data[pos] | (data[pos + 1] << 8);
            pos += 2;
            return {std::to_string(val), -1};
        } else if (format == 2) {
            // 32-bit integer little endian
            if (pos + 4 > max_size) return {"", -1};
            uint32_t val = data[pos] | (data[pos + 1] << 8) | 
                          (data[pos + 2] << 16) | (data[pos + 3] << 24);
            pos += 4;
            return {std::to_string(val), -1};
        }
        return {"", -1};
    }
    
    // normal string encoding
    long long length = parse_length_encoding(data, pos, max_size);
    if (length < 0 || pos + length > max_size) {
        return {"", -1};
    }
    
    std::string str(reinterpret_cast<const char*>(data + pos), length);
    pos += length;
    return {str, -1};
}

bool load_rdb_file() {
    std::string filepath = rdb_config.dir + "/" + rdb_config.dbfilename;
    
    debug_log << "DEBUG RDB: Attempting to load RDB from " << filepath << std::endl;
    
    // check if file exists
    std::ifstream file(filepath, std::ios::binary);
    if (!file) {
        debug_log << "DEBUG RDB: File does not exist, starting with empty database" << std::endl;
        return true;  // not an error just empty database
    }
    
    // read entire file into memory
    file.seekg(0, std::ios::end);
    size_t file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    std::vector<unsigned char> data(file_size);
    file.read(reinterpret_cast<char*>(data.data()), file_size);
    file.close();
    
    debug_log << "DEBUG RDB: Loaded " << file_size << " bytes from RDB file" << std::endl;
    
    size_t pos = 0;
    
    // parse header
    if (pos + 9 > file_size) {
        debug_log << "DEBUG RDB: File too small for header" << std::endl;
        return false;
    }
    
    std::string magic(reinterpret_cast<const char*>(data.data()), 5);
    if (magic != "REDIS") {
        debug_log << "DEBUG RDB: Invalid magic string: " << magic << std::endl;
        return false;
    }
    
    std::string version(reinterpret_cast<const char*>(data.data() + 5), 4);
    debug_log << "DEBUG RDB: Version " << version << std::endl;
    pos = 9;
    
    // parse metadata and database sections
    long long current_time_ms = get_current_time_ms();
    
    while (pos < file_size) {
        if (pos >= file_size) break;
        
        unsigned char op_code = data[pos++];
        
        if (op_code == 0xFF) {
            // eof
            debug_log << "DEBUG RDB: Reached end of file marker" << std::endl;
            break;
        } else if (op_code == 0xFE) {
            // database selector
            long long db_index = parse_length_encoding(data.data(), pos, file_size);
            debug_log << "DEBUG RDB: Database index " << db_index << std::endl;
        } else if (op_code == 0xFB) {
            // hash table size information
            long long hash_table_size = parse_length_encoding(data.data(), pos, file_size);
            long long expire_hash_table_size = parse_length_encoding(data.data(), pos, file_size);
            debug_log << "DEBUG RDB: Hash table size=" << hash_table_size 
                      << ", expire size=" << expire_hash_table_size << std::endl;
        } else if (op_code == 0xFA) {
            // metadata
            auto [name, _] = parse_string_encoding(data.data(), pos, file_size);
            auto [value, __] = parse_string_encoding(data.data(), pos, file_size);
            debug_log << "DEBUG RDB: Metadata " << name << "=" << value << std::endl;
        } else if (op_code == 0xFC || op_code == 0xFD) {
            // key-val pair with expiry
            long long expiry_ms = 0;
            
            if (op_code == 0xFC) {
                // expiry in milliseconds, 8 bytes, little endian
                if (pos + 8 > file_size) break;
                expiry_ms = 0;
                for (int i = 0; i < 8; i++) {
                    expiry_ms |= ((long long)data[pos++] << (i * 8));
                }
            } else {
                // Expiry in seconds, 4 bytes, little endian
                if (pos + 4 > file_size) break;
                long long expiry_sec = 0;
                for (int i = 0; i < 4; i++) {
                    expiry_sec |= ((long long)data[pos++] << (i * 8));
                }
                expiry_ms = expiry_sec * 1000;
            }
            
            // value type
            if (pos >= file_size) break;
            unsigned char value_type = data[pos++];
            
            // key
            auto [key, _] = parse_string_encoding(data.data(), pos, file_size);
            
            // only support string type for now
            if (value_type == 0) {
                auto [value, __] = parse_string_encoding(data.data(), pos, file_size);
                
                // only store if not expired
                if (expiry_ms > current_time_ms) {
                    std::lock_guard<std::mutex> lock(storage_mutex);
                    storageMap[key] = {value, expiry_ms};
                    debug_log << "DEBUG RDB: Loaded key=" << key << " value=" << value 
                              << " expiry=" << expiry_ms << std::endl;
                } else {
                    debug_log << "DEBUG RDB: Skipped expired key=" << key << std::endl;
                }
            }
        } else if (op_code == 0x00) {
            // key-val pair without expiry

            // key
            auto [key, _] = parse_string_encoding(data.data(), pos, file_size);
            
            // value
            auto [value, __] = parse_string_encoding(data.data(), pos, file_size);
            
            std::lock_guard<std::mutex> lock(storage_mutex);
            storageMap[key] = {value, 0};  // means no expiry
            debug_log << "DEBUG RDB: Loaded key=" << key << " value=" << value 
                      << " (no expiry)" << std::endl;
        } else {
            // other value types not supported in this stage
            debug_log << "DEBUG RDB: Skipping unsupported op_code " << (int)op_code << std::endl;
            break;
        }
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    debug_log << "DEBUG RDB: Finished loading RDB file, total keys: " << storageMap.size() << std::endl;
    return true;
}