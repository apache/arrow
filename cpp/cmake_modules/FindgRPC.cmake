# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if( NOT "${GRPC_HOME}" STREQUAL "")
    file (TO_CMAKE_PATH "${GRPC_HOME}" _grpc_path)
endif()

message (STATUS "GRPC_HOME: ${GRPC_HOME}")

find_package(gRPC CONFIG)
if (gRPC_FOUND)
  message (STATUS "Found CMake installation of gRPC")
  get_property(GRPC_INCLUDE_DIR TARGET gRPC::gpr PROPERTY INTERFACE_INCLUDE_DIRECTORIES)
  get_property(GPR_STATIC_LIB TARGET gRPC::gpr PROPERTY LOCATION)
  get_property(GRPC_STATIC_LIB TARGET gRPC::grpc_unsecure PROPERTY LOCATION)
  get_property(GRPCPP_STATIC_LIB TARGET gRPC::grpc++_unsecure PROPERTY LOCATION)
  get_property(GRPC_ADDRESS_SORTING_STATIC_LIB
    TARGET gRPC::address_sorting PROPERTY LOCATION)
  # Get location of grpc_cpp_plugin so we can pass it to protoc
  get_property(GRPC_CPP_PLUGIN TARGET gRPC::grpc_cpp_plugin PROPERTY LOCATION)
else()
  find_path (GRPC_INCLUDE_DIR grpc/grpc.h HINTS
    ${_grpc_path}
    NO_DEFAULT_PATH
    PATH_SUFFIXES "include")

  set (lib_dirs "lib")
  if (EXISTS "${_grpc_path}/lib64")
    set (lib_dirs "lib64" ${lib_dirs})
  endif ()
  if (EXISTS "${_grpc_path}/lib/${CMAKE_LIBRARY_ARCHITECTURE}")
    set (lib_dirs "lib/${CMAKE_LIBRARY_ARCHITECTURE}" ${lib_dirs})
  endif ()

  find_library (GPR_STATIC_LIB
    NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}gpr${CMAKE_STATIC_LIBRARY_SUFFIX}"
    PATHS ${_grpc_path}
    NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})

  # On Debian/Ubuntu, libaddress_sorting is statically linked.
  find_library (GRPC_ADDRESS_SORTING_STATIC_LIB
    NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}address_sorting${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${CMAKE_STATIC_LIBRARY_PREFIX}grpc++${CMAKE_STATIC_LIBRARY_SUFFIX}"
    PATHS ${_grpc_path}
    NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})

  find_library (GRPC_STATIC_LIB
    NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}grpc${CMAKE_STATIC_LIBRARY_SUFFIX}"
    PATHS ${_grpc_path}
    NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})

  find_library (GRPCPP_STATIC_LIB
    NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}grpc++${CMAKE_STATIC_LIBRARY_SUFFIX}"
    PATHS ${_grpc_path}
    NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})

  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin protoc-gen-grpc-cpp
    HINTS ${_grpc_path}
    NO_DEFAULT_PATH
    PATH_SUFFIXES "bin")
endif()

if (GRPC_INCLUDE_DIR AND GPR_STATIC_LIB AND GRPC_ADDRESS_SORTING_STATIC_LIB AND
    GRPC_STATIC_LIB AND GRPCPP_STATIC_LIB AND GRPC_CPP_PLUGIN)
  set (gRPC_FOUND TRUE)
else ()
  set (gRPC_FOUND FALSE)
endif ()

if (gRPC_FOUND)
  message (STATUS "Found the gRPC headers: ${GRPC_INCLUDE_DIR}")
else()
  if (_grpc_path)
    set (GRPC_ERR_MSG "Could not find gRPC. Looked in ${_grpc_path}.")
  else ()
    set (GRPC_ERR_MSG "Could not find gRPC in system search paths.")
  endif()

  if (gRPC_FIND_REQUIRED)
    message (FATAL_ERROR "${GRPC_ERR_MSG}")
  else ()
    message (STATUS "${GRPC_ERR_MSG}")
  endif ()
endif()

mark_as_advanced (
  GRPC_INCLUDE_DIR
  )
