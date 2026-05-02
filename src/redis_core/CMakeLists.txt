add_library(redis-core resp-decoder.cpp resp-encoder.cpp)

target_include_directories(redis-core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
