# Copyright 2018-2020 Peter Dimov
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE_1_0.txt or copy at http://boost.org/LICENSE_1_0.txt)

cmake_minimum_required(VERSION 3.5)

if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

project(CmakeConfigPythonTest LANGUAGES CXX)

include(${CMAKE_CURRENT_LIST_DIR}/../BoostVersion.cmake)

find_package(PythonLibs ${USE_PYTHON_VERSION} REQUIRED)

set(BOOST_HINTS)

if(USE_STAGED_BOOST)
  set(BOOST_HINTS HINTS ../../../../stage)
endif()

set(component python)

if(USE_PYTHON_COMPONENT)

  set(component ${USE_PYTHON_COMPONENT})
  find_package(Boost ${BOOST_VERSION} EXACT REQUIRED COMPONENTS ${component} ${BOOST_HINTS})

else()

  # use the found Python version in case more than one Boost.Python is installed
  set(Boost_PYTHON_VERSION ${PYTHONLIBS_VERSION_STRING})

  if(USE_BOOST_PACKAGE)

    find_package(Boost ${BOOST_VERSION} EXACT REQUIRED COMPONENTS python ${BOOST_HINTS})

  else()

    find_package(boost_python ${BOOST_VERSION} EXACT CONFIG REQUIRED ${BOOST_HINTS})

  endif()
endif()

add_executable(main quick.cpp)
target_link_libraries(main Boost::${component} ${PYTHON_LIBRARIES})
target_include_directories(main PRIVATE ${PYTHON_INCLUDE_DIRS})

enable_testing()
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -C $<CONFIG>)

add_test(main main)
