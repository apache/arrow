# Copyright 2018 John Maddock
# Distributed under the Boost Software License, Version 1.0.
# See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt
#
#  CAUTION!!!!
#
# This addition to Boost.Config should be considered experimental and is part
# of an ongoing effort to add CMake support Boost-wide.
#
# IT IS HIGHLY LIKELY THAT THIS FILE WILL CHANGE WITHOUT NOTICE!!!
#
# DO NOT REPLY ON THE CONTENTS OF THIS FILE!!!
#
# In particular this is not a comprehensive test suite, just a quick check
# that the root CMakeLists.txt works OK, and a convenience for folks who
# want to import these projects into their IDE.
#
cmake_minimum_required(VERSION 3.5)
  
add_subdirectory(.. ${CMAKE_CURRENT_BINARY_DIR}/boost_config)
  
add_executable(config_info config_info.cpp)  
target_link_libraries(config_info Boost::config)  

add_executable(config_test config_test.cpp)  
target_link_libraries(config_test Boost::config)  

