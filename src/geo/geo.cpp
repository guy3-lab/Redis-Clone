#include "geo.h"
#include "../protocol/resp_parser.h"
#include "../storage/sorted_sets.h"
#include "../storage/storage.h"

// validate longitude and latitude
bool validate_coordinates(double longitude, double latitude, std::string& error_msg) {
    if (longitude < GEO_LON_MIN || longitude > GEO_LON_MAX) {
        error_msg = "ERR invalid longitude,latitude pair " + 
                    std::to_string(longitude) + "," + std::to_string(latitude);
        return false;
    }
    
    if (latitude < GEO_LAT_MIN || latitude > GEO_LAT_MAX) {
        error_msg = "ERR invalid longitude,latitude pair " + 
                    std::to_string(longitude) + "," + std::to_string(latitude);
        return false;
    }
    
    return true;
}

// Encode longitude and latitude to geohash (52-bit)
long long encode_geohash(double longitude, double latitude) {
    debug_log << "\n=== GEOHASH ENCODE START ===" << std::endl;
    debug_log << "GEOHASH_INPUT: lon=" << longitude << " lat=" << latitude << std::endl;
    
    double lat_offset = (latitude - GEO_LAT_MIN);
    double lon_offset = (longitude - GEO_LON_MIN);
    
    debug_log << "GEOHASH_OFFSET: lon_offset=" << lon_offset << " lat_offset=" << lat_offset << std::endl;
    
    // normalize
    lat_offset /= (GEO_LAT_MAX - GEO_LAT_MIN);
    lon_offset /= (GEO_LON_MAX - GEO_LON_MIN);
    
    debug_log << "GEOHASH_NORMALIZED: lon=" << lon_offset << " lat=" << lat_offset << std::endl;
    
    // convert to 52 bit geohash
    long long lat_int = (long long)(lat_offset * (1LL << 26));
    long long lon_int = (long long)(lon_offset * (1LL << 26));
    
    debug_log << "GEOHASH_INT: lon_int=" << lon_int << " lat_int=" << lat_int << std::endl;
    
    // clamp to 26 bits
    if (lat_int >= (1LL << 26)) lat_int = (1LL << 26) - 1;
    if (lon_int >= (1LL << 26)) lon_int = (1LL << 26) - 1;
    
    long long geohash = 0;
    
    // interleave bits
    for (int i = 25; i >= 0; i--) {
        geohash <<= 1;
        geohash |= (lon_int >> i) & 1;
        geohash <<= 1;
        geohash |= (lat_int >> i) & 1;
    }
    
    debug_log << "GEOHASH_OUTPUT: hash=" << geohash << std::endl;
    debug_log << "GEOHASH_EXPECTED (London): 3662244577883556" << std::endl;
    debug_log << "GEOHASH_EXPECTED (Paris): 3663832614298053" << std::endl;
    debug_log << "=== GEOHASH ENCODE END ===\n" << std::endl;
    
    return geohash;
}

// decode geohash back to longitude and latitude
GeoCoordinates decode_geohash(long long hash) {
    long long lat_int = 0;
    long long lon_int = 0;
    
    for (int i = 0; i < 52; i++) {
        if (i % 2 == 0) {
            lon_int = (lon_int << 1) | ((hash >> (51 - i)) & 1);
        } else {
            lat_int = (lat_int << 1) | ((hash >> (51 - i)) & 1);
        }
    }
    
    // convert back to coordinates
    double lat_offset = (double)lat_int / (1LL << 26);
    double lon_offset = (double)lon_int / (1LL << 26);
    
    double latitude = lat_offset * (GEO_LAT_MAX - GEO_LAT_MIN) + GEO_LAT_MIN;
    double longitude = lon_offset * (GEO_LON_MAX - GEO_LON_MIN) + GEO_LON_MIN;
    
    return {longitude, latitude};
}

// calculate distance using Haversine
double calculate_distance(double lon1, double lat1, double lon2, double lat2) {
    double lat1_rad = lat1 * M_PI / 180.0;
    double lat2_rad = lat2 * M_PI / 180.0;
    double lon1_rad = lon1 * M_PI / 180.0;
    double lon2_rad = lon2 * M_PI / 180.0;
    
    double dlat = lat2_rad - lat1_rad;
    double dlon = lon2_rad - lon1_rad;
    
    double a = sin(dlat / 2) * sin(dlat / 2) +
               cos(lat1_rad) * cos(lat2_rad) *
               sin(dlon / 2) * sin(dlon / 2);
    
    double c = 2 * atan2(sqrt(a), sqrt(1 - a));
    
    return EARTH_RADIUS_METERS * c;
}

// GEOADD command handler
std::string handle_geoadd(const std::vector<std::string>& args) {
    if (args.size() < 5 || (args.size() - 2) % 3 != 0) {
        return encode_error("ERR wrong number of arguments for 'geoadd' command");
    }
    
    std::string key = args[1];
    int added_count = 0;
    
    // process longitude-latitude-member triplets
    for (size_t i = 2; i < args.size(); i += 3) {
        double longitude = std::stod(args[i]);
        double latitude = std::stod(args[i + 1]);
        std::string member = args[i + 2];
        
        std::string error_msg;
        if (!validate_coordinates(longitude, latitude, error_msg)) {
            return encode_error(error_msg);
        }
        
        long long score = encode_geohash(longitude, latitude);
        
        std::lock_guard<std::mutex> lock(storage_mutex);
        
        bool is_new = (sortedSetStorage[key].member_scores.count(member) == 0);
        
        if (is_new) {
            added_count++;
        } else {
            double old_score = sortedSetStorage[key].member_scores[member];
            sortedSetStorage[key].sorted_members.erase({old_score, member});
        }
        
        sortedSetStorage[key].member_scores[member] = score;
        sortedSetStorage[key].sorted_members.insert({(double)score, member});
    }
    
    return encode_integer(added_count);
}

// GEOPOS command handler
std::string handle_geopos(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        return encode_error("ERR wrong number of arguments for 'geopos' command");
    }
    
    std::string key = args[1];
    std::string response = "*" + std::to_string(args.size() - 2) + "\r\n";
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    // process each requested member
    for (size_t i = 2; i < args.size(); i++) {
        std::string member = args[i];
        
        if (sortedSetStorage.count(key) == 0 || 
            sortedSetStorage[key].member_scores.count(member) == 0) {
            response += "*-1\r\n";
        } else {
            long long score = (long long)sortedSetStorage[key].member_scores[member];
            GeoCoordinates coords = decode_geohash(score);
            
            response += "*2\r\n";
            response += encode_bulk_string(std::to_string(coords.longitude));
            response += encode_bulk_string(std::to_string(coords.latitude));
        }
    }
    
    return response;
}

// GEODIST command handler
std::string handle_geodist(const std::vector<std::string>& args) {
    if (args.size() < 4) {
        return encode_error("ERR wrong number of arguments for 'geodist' command");
    }
    
    std::string key = args[1];
    std::string member1 = args[2];
    std::string member2 = args[3];
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    // check if key exists
    if (sortedSetStorage.count(key) == 0) {
        return "$-1\r\n";
    }
    
    // check if both members exist
    if (sortedSetStorage[key].member_scores.count(member1) == 0 ||
        sortedSetStorage[key].member_scores.count(member2) == 0) {
        return "$-1\r\n";
    }
    
    long long score1 = (long long)sortedSetStorage[key].member_scores[member1];
    long long score2 = (long long)sortedSetStorage[key].member_scores[member2];
    
    GeoCoordinates coords1 = decode_geohash(score1);
    GeoCoordinates coords2 = decode_geohash(score2);
    
    // calculate haversine
    double distance = calculate_distance(coords1.longitude, coords1.latitude,
                                         coords2.longitude, coords2.latitude);
    
    // return distance as bulk string
    return encode_bulk_string(std::to_string(distance));
}

// GEOSEARCH command handler
std::string handle_geosearch(const std::vector<std::string>& args) {
    if (args.size() < 7) {
        return encode_error("ERR wrong number of arguments for 'geosearch' command");
    }
    
    std::string key = args[1];
    
    // parse fromlonlat
    if (args[2] != "FROMLONLAT" && args[2] != "fromlonlat") {
        return encode_error("ERR FROMLONLAT required");
    }
    
    double center_lon = std::stod(args[3]);
    double center_lat = std::stod(args[4]);
    
    // parse byradius
    if (args[5] != "BYRADIUS" && args[5] != "byradius") {
        return encode_error("ERR BYRADIUS required");
    }
    
    double radius = std::stod(args[6]);
    std::string unit = args[7];
    
    // convert radius to meters
    double radius_meters = radius;
    if (unit == "km") {
        radius_meters = radius * 1000.0;
    } else if (unit == "mi") {
        radius_meters = radius * 1609.34;
    } else if (unit == "ft") {
        radius_meters = radius * 0.3048;
    }
    
    std::lock_guard<std::mutex> lock(storage_mutex);
    
    if (sortedSetStorage.count(key) == 0) {
        return "*0\r\n";
    }
    
    std::vector<std::string> results;
    
    for (const auto& [member, score] : sortedSetStorage[key].member_scores) {
        long long member_hash = (long long)score;
        GeoCoordinates member_coords = decode_geohash(member_hash);
        
        double distance = calculate_distance(center_lon, center_lat,
                                             member_coords.longitude, member_coords.latitude);
        
        if (distance <= radius_meters) {
            results.push_back(member);
        }
    }
    
    // build response
    std::string response = "*" + std::to_string(results.size()) + "\r\n";
    for (const auto& member : results) {
        response += encode_bulk_string(member);
    }
    
    return response;
}