include(FetchContent)
set(FETCHCONTENT_QUIET FALSE)
FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.17.0
)
FetchContent_MakeAvailable(googletest)
enable_testing()

set(TARGET_NAME redis-tests)

add_executable(${TARGET_NAME} radix-trie-tests.cpp)

target_link_libraries(${TARGET_NAME}
        PRIVATE
        GTest::gtest_main
        GTest::gmock
        redis-storage)

include(GoogleTest)
gtest_discover_tests(${TARGET_NAME})
