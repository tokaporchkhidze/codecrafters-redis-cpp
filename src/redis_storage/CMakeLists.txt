add_library(redis-storage OBJECT
        redis-map.cpp
        redis-store.cpp)

target_include_directories(redis-storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)