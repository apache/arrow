# Copyright 2018-2020 Peter Dimov
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE_1_0.txt or copy at http://boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 3.5)

if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

project(CmakeConfigFilesystemTest LANGUAGES CXX)

include(${CMAKE_CURRENT_LIST_DIR}/../BoostVersion.cmake)

set(BOOST_HINTS)

if(USE_STAGED_BOOST)
  set(BOOST_HINTS HINTS ../../../../stage)
endif()

if(USE_ALL_COMPONENTS)

  find_package(Boost ${BOOST_VERSION} EXACT REQUIRED ALL ${BOOST_HINTS})

elseif(USE_ALL_COMPONENTS_CONFIG)

  find_package(Boost ${BOOST_VERSION} EXACT CONFIG REQUIRED ALL ${BOOST_HINTS})

elseif(USE_BOOST_PACKAGE)

  find_package(Boost ${BOOST_VERSION} EXACT REQUIRED COMPONENTS filesystem ${BOOST_HINTS})

else()

  find_package(boost_filesystem ${BOOST_VERSION} EXACT CONFIG REQUIRED ${BOOST_HINTS})

endif()

add_executable(main quick.cpp)
target_link_libraries(main Boost::filesystem)

enable_testing()
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -C $<CONFIG>)

add_test(main main)
