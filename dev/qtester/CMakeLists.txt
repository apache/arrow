cmake_minimum_required(VERSION 3.19)
project(arrow-query-tester)

set(CMAKE_CXX_STANDARD 17)
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")
endif()
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(ExternalProject)

# Add Arrow
find_package(Arrow REQUIRED COMPONENTS dataset parquet engine)
# Argparse is a modern library for interpreting CLI args
set(ARGPARSE_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/argparse_ep-install")
set(ARGPARSE_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARGPARSE_PREFIX}")
set(ARGPARSE_INCLUDE_DIR "${ARGPARSE_PREFIX}/include")
externalproject_add(argparse
                    CMAKE_ARGS ${ARGPARSE_CMAKE_ARGS}
                    INSTALL_DIR ${ARGPARSE_PREFIX}
                    URL https://github.com/p-ranav/argparse/archive/refs/tags/v2.2.tar.gz
                    URL_HASH "SHA256=f0fc6ab7e70ac24856c160f44ebb0dd79dc1f7f4a614ee2810d42bb73799872b")

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_CLANG_TIDY "clang-tidy-12")
endif()

function(ADD_PROGRAM TARGET)
    set(options)
    set(one_value_args)
    set(multi_value_args EXTRA_SOURCES)
    cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
    add_executable(
            ${TARGET}
            ${TARGET}.cc
            ${ARG_EXTRA_SOURCES}
    )
    add_dependencies(${TARGET} argparse)
    target_include_directories(${TARGET} SYSTEM PRIVATE "${ARGPARSE_INCLUDE_DIR}")
    target_link_libraries(
            ${TARGET}
            arrow_shared
            arrow_dataset
            arrow_engine
            parquet
    )
    if (MSVC)
        target_compile_options(${TARGET} PRIVATE /W4 /WX)
    else ()
        target_compile_options(${TARGET} PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif ()

endfunction()

add_program(query_tester EXTRA_SOURCES builtin_queries.cc test_runner.cc)
