
cmake_minimum_required(VERSION 3.16)
project(sentinel_simulator CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(MOSQ REQUIRED libmosquitto)

find_path(CJSON_INCLUDE_DIR
    NAMES cjson/cJSON.h
    PATHS /yourpath/cjson/include
    REQUIRED
)

find_library(MOSQUITTO_LIBRARY
    NAMES mosquitto
    PATHS /yourpath/mosquitto/lib
    REQUIRED
)

add_executable(meter_sim src/main.cpp)

target_include_directories(meter_sim PRIVATE
    ${MOSQ_INCLUDE_DIRS}
    ${CJSON_INCLUDE_DIR}
)

target_link_libraries(meter_sim PRIVATE
    ${MOSQUITTO_LIBRARY}
    pthread
)


# opt/homebrew/opt/