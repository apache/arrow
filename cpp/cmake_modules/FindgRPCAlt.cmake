#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(find_package_args)
if(gRPCAlt_FIND_VERSION)
  list(APPEND find_package_args ${gRPCAlt_FIND_VERSION})
endif()
if(gRPCAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(gRPC ${find_package_args})
if(gRPC_FOUND)
  set(gRPCAlt_FOUND TRUE)
  return()
endif()

unset(GRPC_ALT_VERSION)

if(ARROW_GRPC_USE_SHARED)
  set(GRPC_GPR_LIB_NAMES)
  set(GRPC_GRPC_LIB_NAMES)
  set(GRPC_GRPCPP_LIB_NAMES)
  set(GRPC_ADDRESS_SORTING_LIB_NAMES)
  set(GRPC_UPB_LIB_NAMES)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(APPEND GRPC_GPR_LIB_NAMES
                "${CMAKE_IMPORT_LIBRARY_PREFIX}gpr${CMAKE_IMPORT_LIBRARY_SUFFIX}")
    list(APPEND GRPC_GRPC_LIB_NAMES
                "${CMAKE_IMPORT_LIBRARY_PREFIX}grpc${CMAKE_IMPORT_LIBRARY_SUFFIX}")
    list(APPEND GRPC_GRPCPP_LIB_NAMES
                "${CMAKE_IMPORT_LIBRARY_PREFIX}grpc++${CMAKE_IMPORT_LIBRARY_SUFFIX}")
    list(
      APPEND GRPC_ADDRESS_SORTING_LIB_NAMES
             "${CMAKE_IMPORT_LIBRARY_PREFIX}address_sorting${CMAKE_IMPORT_LIBRARY_SUFFIX}"
      )
    list(APPEND GRPC_UPB_LIB_NAMES
                "${CMAKE_IMPORT_LIBRARY_PREFIX}upb${CMAKE_IMPORT_LIBRARY_SUFFIX}")
  endif()
  list(APPEND GRPC_GPR_LIB_NAMES
              "${CMAKE_SHARED_LIBRARY_PREFIX}gpr${CMAKE_SHARED_LIBRARY_SUFFIX}")
  list(APPEND GRPC_GRPC_LIB_NAMES
              "${CMAKE_SHARED_LIBRARY_PREFIX}grpc${CMAKE_SHARED_LIBRARY_SUFFIX}")
  list(APPEND GRPC_GRPCPP_LIB_NAMES
              "${CMAKE_SHARED_LIBRARY_PREFIX}grpc++${CMAKE_SHARED_LIBRARY_SUFFIX}")
  list(
    APPEND GRPC_ADDRESS_SORTING_LIB_NAMES
           "${CMAKE_SHARED_LIBRARY_PREFIX}address_sorting${CMAKE_SHARED_LIBRARY_SUFFIX}")
  list(APPEND GRPC_UPB_LIB_NAMES
              "${CMAKE_SHARED_LIBRARY_PREFIX}upb${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
  set(GRPC_GPR_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}gpr${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(GRPC_GRPC_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}grpc${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(GRPC_GRPCPP_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}grpc++${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(GRPC_ADDRESS_SORTING_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}address_sorting${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(GRPC_UPB_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}upb${CMAKE_STATIC_LIBRARY_SUFFIX}")
endif()

if(gRPC_ROOT)
  find_library(GRPC_GPR_LIB
               NAMES ${GRPC_GPR_LIB_NAMES}
               PATHS ${gRPC_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_library(GRPC_GRPC_LIB
               NAMES ${GRPC_GRPC_LIB_NAMES}
               PATHS ${gRPC_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_library(GRPC_GRPCPP_LIB
               NAMES ${GRPC_GRPCPP_LIB_NAMES}
               PATHS ${gRPC_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_library(GRPC_ADDRESS_SORTING_LIB
               NAMES ${GRPC_ADDRESS_SORTING_LIB_NAMES}
               PATHS ${gRPC_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_library(GRPC_UPB_LIB
               NAMES ${GRPC_UPB_LIB_NAMES}
               PATHS ${gRPC_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin NO_DEFAULT_PATH
               PATHS ${gRPC_ROOT}
               PATH_SUFFIXES "bin")
  find_path(GRPC_INCLUDE_DIR
            NAMES grpc/grpc.h
            PATHS ${gRPC_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_package(PkgConfig QUIET)
  pkg_check_modules(GRPC_PC grpc++)
  if(GRPC_PC_FOUND)
    set(GRPC_ALT_VERSION "${GRPC_PC_VERSION}")
    set(GRPC_INCLUDE_DIR "${GRPC_PC_INCLUDEDIR}")
    list(APPEND GRPC_PC_LIBRARY_DIRS "${GRPC_PC_LIBDIR}")
    message(STATUS "${GRPC_PC_LIBRARY_DIRS}")

    find_library(GRPC_GPR_LIB
                 NAMES ${GRPC_GPR_LIB_NAMES}
                 PATHS ${GRPC_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(GRPC_GRPC_LIB
                 NAMES ${GRPC_GRPC_LIB_NAMES}
                 PATHS ${GRPC_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(GRPC_GRPCPP_LIB
                 NAMES ${GRPC_GRPCPP_LIB_NAMES}
                 PATHS ${GRPC_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(GRPC_ADDRESS_SORTING_LIB
                 NAMES ${GRPC_ADDRESS_SORTING_LIB_NAMES}
                 PATHS ${GRPC_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(GRPC_UPB_LIB
                 NAMES ${GRPC_UPB_LIB_NAMES}
                 PATHS ${GRPC_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
                 HINTS ${GRPC_PC_PREFIX}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES "bin")
  else()
    find_library(GRPC_GPR_LIB
                 NAMES ${GRPC_GPR_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_library(GRPC_GRPC_LIB
                 NAMES ${GRPC_GRPC_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_library(GRPC_GRPCPP_LIB
                 NAMES ${GRPC_GRPCPP_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_library(GRPC_ADDRESS_SORTING_LIB
                 NAMES ${GRPC_ADDRESS_SORTING_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_library(GRPC_UPB_LIB
                 NAMES ${GRPC_UPB_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin PATH_SUFFIXES "bin")
    find_path(GRPC_INCLUDE_DIR
              NAMES grpc/grpc.h
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
endif()

set(GRPC_ALT_FIND_PACKAGE_ARGS
    gRPCAlt
    REQUIRED_VARS
    GRPC_INCLUDE_DIR
    GRPC_GPR_LIB
    GRPC_GRPC_LIB
    GRPC_GRPCPP_LIB
    GRPC_CPP_PLUGIN)
if(GRPC_ALT_VERSION)
  list(APPEND GRPC_ALT_FIND_PACKAGE_ARGS VERSION_VAR GRPC_ALT_VERSION)
endif()
find_package_handle_standard_args(${GRPC_ALT_FIND_PACKAGE_ARGS})

if(gRPCAlt_FOUND)
  add_library(gRPC::gpr UNKNOWN IMPORTED)
  set_target_properties(gRPC::gpr
                        PROPERTIES IMPORTED_LOCATION "${GRPC_GPR_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc UNKNOWN IMPORTED)
  set_target_properties(
    gRPC::grpc
    PROPERTIES IMPORTED_LOCATION
               "${GRPC_GRPC_LIB}"
               INTERFACE_INCLUDE_DIRECTORIES
               "${GRPC_INCLUDE_DIR}"
               INTERFACE_LINK_LIBRARIES
               "OpenSSL::SSL;OpenSSL::Crypto;ZLIB::ZLIB;c-ares::cares")

  set(_GRPCPP_LINK_LIBRARIES "gRPC::grpc;gRPC::gpr")

  if(GRPC_ADDRESS_SORTING_LIB)
    # Address sorting is optional and not always required.
    add_library(gRPC::address_sorting UNKNOWN IMPORTED)
    set_target_properties(gRPC::address_sorting
                          PROPERTIES IMPORTED_LOCATION "${GRPC_ADDRESS_SORTING_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")
    set(_GRPCPP_LINK_LIBRARIES "${_GRPCPP_LINK_LIBRARIES};gRPC::address_sorting")
  endif()

  if(GRPC_UPB_LIB)
    # upb is used by recent gRPC versions
    add_library(gRPC::upb UNKNOWN IMPORTED)
    set_target_properties(gRPC::upb
                          PROPERTIES IMPORTED_LOCATION "${GRPC_UPB_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")
    set(_GRPCPP_LINK_LIBRARIES "${_GRPCPP_LINK_LIBRARIES};gRPC::upb")
  endif()

  find_package(absl CONFIG)
  if(absl_FOUND)
    # Abseil libraries that recent gRPC versions depend on
    set(_ABSL_LIBS
        bad_optional_access
        int128
        raw_logging_internal
        str_format_internal
        strings
        throw_delegate
        time
        time_zone)

    foreach(_ABSL_LIB ${_ABSL_LIBS})
      set(_GRPCPP_LINK_LIBRARIES "${_GRPCPP_LINK_LIBRARIES};absl::${_ABSL_LIB}")
    endforeach()
  endif()

  add_library(gRPC::grpc++ UNKNOWN IMPORTED)
  set_target_properties(gRPC::grpc++
                        PROPERTIES IMPORTED_LOCATION
                                   "${GRPC_GRPCPP_LIB}"
                                   INTERFACE_LINK_LIBRARIES
                                   "${_GRPCPP_LINK_LIBRARIES}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${GRPC_INCLUDE_DIR}")

  add_executable(gRPC::grpc_cpp_plugin IMPORTED)
  set_target_properties(gRPC::grpc_cpp_plugin
                        PROPERTIES IMPORTED_LOCATION ${GRPC_CPP_PLUGIN})
endif()
