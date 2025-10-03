#ifndef GEO_H
#define GEO_H

#include "../server.h"
#include <cmath>

// constants for geohashing
const double GEO_LAT_MIN = -85.05112878;
const double GEO_LAT_MAX = 85.05112878;
const double GEO_LON_MIN = -180.0;
const double GEO_LON_MAX = 180.0;
const double EARTH_RADIUS_METERS = 6372797.560856;

struct GeoCoordinates {
    double longitude;
    double latitude;
};

// geohashing funcs
long long encode_geohash(double longitude, double latitude);
GeoCoordinates decode_geohash(long long hash);
bool validate_coordinates(double longitude, double latitude, std::string& error_msg);
double calculate_distance(double lon1, double lat1, double lon2, double lat2);

// command handlers
std::string handle_geoadd(const std::vector<std::string>& args);
std::string handle_geopos(const std::vector<std::string>& args);
std::string handle_geodist(const std::vector<std::string>& args);
std::string handle_geosearch(const std::vector<std::string>& args);

#endif