# Copyright Mike Dev 2018
# Copyright Rene Rivera 2018
# Distributed under the Boost Software License, Version 1.0.
# See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt

# NOTE:
#   This does NOT run the unit tests for Boost.Predef.
#   It only tests, if the CMakeLists.txt file in predef's
#   root directory works as expected (i.e. it provides the
#   target Boost::predef which in turn provides the
#   correct include directory )

# We have very simple cmake requirements we we still require the new style
# declarative targets.
cmake_minimum_required( VERSION 3.0 )

project( PredefCMakeSelfTest )

# Process cmake file at root of library and use
# ${CMAKE_CURRENT_BINARY_DIR}/libs/predef as workspace
add_subdirectory( ../.. ${CMAKE_CURRENT_BINARY_DIR}/libs/predef )

# The executable just includes a predef header to verify that it's used.
add_executable( predef_cmake_test_prj main.cpp )

# The executable needs to "use" the Predef "library" to get the usage
# requirements added to the executable build.
target_link_libraries( predef_cmake_test_prj Boost::predef )
