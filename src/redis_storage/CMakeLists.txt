add_library(redis-storage OBJECT
        redis-store.cpp
        redis-stream.cpp)

target_include_directories(redis-storage PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)