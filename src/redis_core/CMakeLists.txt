file(GLOB COMMAND_SOURCES CONFIGURE_DEPENDS "commands/*.cpp")
file(GLOB SERVICE_SOURCES CONFIGURE_DEPENDS "services/*.cpp")

add_library(redis-core OBJECT
        resp-decoder.cpp
        resp-encoder.cpp
        redis-command.cpp
        redis-executor.cpp
        reply-builder.cpp
        ${COMMAND_SOURCES}
        ${SERVICE_SOURCES})

target_include_directories(redis-core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
