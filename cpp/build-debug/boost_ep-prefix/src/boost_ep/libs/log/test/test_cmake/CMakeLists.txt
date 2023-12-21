# Copyright 2021 Andrey Semashev
#
# Distributed under the Boost Software License, Version 1.0.
# See accompanying file LICENSE_1_0.txt or copy at https://www.boost.org/LICENSE_1_0.txt
#
# NOTE: This does NOT run the unit tests for Boost.Log.
#       It only tests if the CMakeLists.txt file in its root works as expected

cmake_minimum_required(VERSION 3.5)

project(BoostLogCMakeSelfTest)

# Use experimental superproject to pull library dependencies recursively
set(BOOST_ENABLE_CMAKE 1)
add_subdirectory(../../../.. "${CMAKE_CURRENT_BINARY_DIR}/boost_superproject")

add_definitions(-DBOOST_ALL_NO_LIB)

if (BOOST_LOG_WITHOUT_SETTINGS_PARSERS)
    add_definitions(-DBOOST_LOG_WITHOUT_SETTINGS_PARSERS)
endif()

add_executable(boost_log_cmake_self_test log_test.cpp)
target_link_libraries(boost_log_cmake_self_test Boost::log)

add_executable(boost_log_setup_cmake_self_test log_setup_test.cpp)
target_link_libraries(boost_log_setup_cmake_self_test Boost::log_setup)
