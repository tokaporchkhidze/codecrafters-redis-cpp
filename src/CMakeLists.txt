add_subdirectory(redis_core)
add_subdirectory(redis_net)

add_executable(redis main.cpp)

set_target_properties(redis PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

target_link_libraries(redis PRIVATE redis-net redis-core)