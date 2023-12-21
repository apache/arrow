# Copyright 2018, 2019 Peter Dimov
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE_1_0.txt or copy at http://boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 3.5)

if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

project(CmakeConfigSerializationTest LANGUAGES CXX)

include(${CMAKE_CURRENT_LIST_DIR}/../BoostVersion.cmake)

set(BOOST_HINTS)

if(USE_STAGED_BOOST)
  set(BOOST_HINTS HINTS ../../../../stage)
endif()

if(USE_BOOST_PACKAGE)

  find_package(Boost ${BOOST_VERSION} EXACT REQUIRED COMPONENTS serialization wserialization ${BOOST_HINTS})

else()

  find_package(boost_serialization ${BOOST_VERSION} EXACT CONFIG REQUIRED ${BOOST_HINTS})
  find_package(boost_wserialization ${BOOST_VERSION} EXACT CONFIG REQUIRED ${BOOST_HINTS})

endif()

add_executable(quick quick.cpp)
target_link_libraries(quick Boost::serialization)

add_executable(wquick wquick.cpp)
target_link_libraries(wquick Boost::wserialization)

enable_testing()
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -C $<CONFIG>)

add_test(quick quick)
add_test(wquick wquick)
