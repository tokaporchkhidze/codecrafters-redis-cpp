add_library(redis-net OBJECT
        tcp-connection.cpp
        tcp-server.cpp
        master-link.cpp
        event-loop.cpp)

target_include_directories(redis-net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
