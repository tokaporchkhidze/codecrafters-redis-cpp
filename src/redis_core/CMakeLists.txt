add_library(redis-core OBJECT
        resp-decoder.cpp
        resp-encoder.cpp
        redis-command.cpp
        redis-executor.cpp)

target_include_directories(redis-core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
