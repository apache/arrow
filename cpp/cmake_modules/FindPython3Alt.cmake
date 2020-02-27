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

# This module finds the libraries corresponding to the Python 3 interpeter,
# and sets the following variables:
# - PYTHON_EXECUTABLE
# - PYTHON_INCLUDE_DIRS
# - PYTHON_LIBRARIES
# - PYTHON_OTHER_LIBS

# Need CMake 3.15 or later for Python3_FIND_STRATEGY
if(${CMAKE_VERSION} VERSION_LESS "3.15.0")
  # Use deprecated FindPythonInterp package
  if(Python3Alt_FIND_REQUIRED)
    find_package(PythonLibsNew REQUIRED)
  else()
    find_package(PythonLibsNew)
  endif()
  return()
endif()

if(Python3Alt_FIND_REQUIRED)
  find_package(Python3 COMPONENTS Interpreter Development NumPy REQUIRED)
else()
  find_package(Python3 COMPONENTS Interpreter Development NumPy)
endif()

if(NOT Python3_FOUND)
  return()
endif()

set(PYTHON_EXECUTABLE ${Python3_EXECUTABLE})
set(PYTHON_INCLUDE_DIRS ${Python3_INCLUDE_DIRS})
set(PYTHON_LIBRARIES ${Python3_LIBRARIES})
set(PYTHON_OTHER_LIBS)

message(STATUS "Found Python 3 libraries: ${PYTHON_LIBRARIES}")

function(PYTHON_ADD_MODULE _NAME)
  python3_add_library(${_NAME} ${ARGN})
endfunction()
