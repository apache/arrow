# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This module finds the libraries corresponding to the Python 3 interpreter
# and the NumPy package, and sets the following variables:
# - PYTHON_EXECUTABLE
# - PYTHON_INCLUDE_DIRS
# - PYTHON_LIBRARIES
# - PYTHON_OTHER_LIBS
# - NUMPY_INCLUDE_DIRS

# Need CMake 3.15 or later for Python3_FIND_STRATEGY
if(${CMAKE_VERSION} VERSION_LESS "3.15.0")
  # Use deprecated Python- and NumPy-finding code
  if(Python3Alt_FIND_REQUIRED)
    find_package(PythonLibsNew REQUIRED)
    find_package(NumPy REQUIRED)
  else()
    find_package(PythonLibsNew)
    find_package(NumPy)
  endif()
  find_package_handle_standard_args(Python3Alt
                                    REQUIRED_VARS
                                    PYTHON_EXECUTABLE
                                    PYTHON_INCLUDE_DIRS
                                    NUMPY_INCLUDE_DIRS)
  return()
endif()

if(${CMAKE_VERSION} VERSION_LESS "3.18.0" OR ARROW_BUILD_TESTS)
  # When building arrow-python-test, we need libpython to be present, so ask for
  # the full "Development" component.  Also ask for it on CMake < 3.18,
  # where "Development.Module" is not available.
  if(Python3Alt_FIND_REQUIRED)
    find_package(Python3 COMPONENTS Interpreter Development NumPy REQUIRED)
  else()
    find_package(Python3 COMPONENTS Interpreter Development NumPy)
  endif()
else()
  if(Python3Alt_FIND_REQUIRED)
    find_package(Python3 COMPONENTS Interpreter Development.Module NumPy REQUIRED)
  else()
    find_package(Python3 COMPONENTS Interpreter Development.Module NumPy)
  endif()
endif()

if(NOT Python3_FOUND)
  return()
endif()

set(PYTHON_EXECUTABLE ${Python3_EXECUTABLE})
set(PYTHON_INCLUDE_DIRS ${Python3_INCLUDE_DIRS})
set(PYTHON_LIBRARIES ${Python3_LIBRARIES})
set(PYTHON_OTHER_LIBS)

get_target_property(NUMPY_INCLUDE_DIRS Python3::NumPy INTERFACE_INCLUDE_DIRECTORIES)

# CMake's python3_add_library() doesn't apply the required extension suffix,
# detect it ourselves.
# (https://gitlab.kitware.com/cmake/cmake/issues/20408)
execute_process(
  COMMAND "${PYTHON_EXECUTABLE}" "-c"
          "from distutils import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"
  RESULT_VARIABLE _PYTHON_RESULT
  OUTPUT_VARIABLE _PYTHON_STDOUT
  ERROR_VARIABLE _PYTHON_STDERR)

if(NOT _PYTHON_RESULT MATCHES 0)
  if(Python3Alt_FIND_REQUIRED)
    message(FATAL_ERROR "Python 3 config failure:\n${_PYTHON_STDERR}")
  endif()
endif()

string(STRIP ${_PYTHON_STDOUT} _EXT_SUFFIX)

function(PYTHON_ADD_MODULE name)
  python3_add_library(${name} MODULE ${ARGN})
  set_target_properties(${name} PROPERTIES SUFFIX ${_EXT_SUFFIX})
endfunction()

find_package_handle_standard_args(Python3Alt
                                  REQUIRED_VARS
                                  PYTHON_EXECUTABLE
                                  PYTHON_INCLUDE_DIRS
                                  NUMPY_INCLUDE_DIRS)
