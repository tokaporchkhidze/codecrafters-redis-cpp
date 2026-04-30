cmake_minimum_required(VERSION 3.13)

project(redis-starter-cpp LANGUAGES CXX)

file(GLOB_RECURSE SOURCE_FILES src/*.cpp)

set(CMAKE_CXX_STANDARD 23) # Enable the C++23 standard
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)

find_package(Threads REQUIRED)

option(ASIO_USE_CPM "Download Asio with CPM instead of FetchContent" OFF)
option(
        ASIO_CPM_FETCHCONTENT_COMPAT
        "Should asio be declared with FetchContent functions to be compatible. This doesn't not allow CPM cache to work."
        ON
)
# Download this repository
include(FetchContent)
FetchContent_Declare(
        asiocmake
        GIT_REPOSITORY "https://github.com/OlivierLDff/asio.cmake"
        GIT_TAG        "main"
)
FetchContent_MakeAvailable(asiocmake)

add_executable(redis ${SOURCE_FILES})

target_link_libraries(redis PRIVATE asio asio::asio)
target_link_libraries(redis PRIVATE Threads::Threads)