# Copyright 2018, 2019 Peter Dimov
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE_1_0.txt or copy at http://boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 3.2)

if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

project(CmakeConfigHeadersTest LANGUAGES CXX)

include(${CMAKE_CURRENT_LIST_DIR}/../BoostVersion.cmake)

set(BOOST_HINTS)

if(USE_STAGED_BOOST)
  set(BOOST_HINTS HINTS ../../../../stage)
endif()

find_package(Boost ${BOOST_VERSION} EXACT REQUIRED ${BOOST_HINTS})

# Using `include_directories` is bad practice, done here for
# testing purposes. The right, "modern CMake", thing to do is
# `target_link_libraries(main Boost::headers)`.

include_directories(${Boost_INCLUDE_DIRS})

add_executable(main quick.cpp)

enable_testing()
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -C $<CONFIG>)

add_test(main main)
