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

pkg_check_modules(GRPC_PC grpc)
if(GRPC_PC_FOUND)
  set(GRPC_INCLUDE_DIR "${GRPC_PC_INCLUDEDIR}")
  list(APPEND GRPC_PC_LIBRARY_DIRS "${GRPC_PC_LIBDIR}")
  message(STATUS "${GRPC_PC_LIBRARY_DIRS}")

  find_library(GRPC_GPR_LIB gpr PATHS ${GRPC_PC_LIBRARY_DIRS} NO_DEFAULT_PATH)
  find_library(GRPC_GRPC_LIB grpc PATHS ${GRPC_PC_LIBRARY_DIRS} NO_DEFAULT_PATH)
  find_library(GRPC_GRPCPP_LIB grpc++ PATHS ${GRPC_PC_LIBRARY_DIRS} NO_DEFAULT_PATH)
  find_library(GRPC_ADDRESS_SORTING_LIB address_sorting
               PATHS ${GRPC_PC_LIBRARY_DIRS}
               NO_DEFAULT_PATH)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin
               HINTS ${GRPC_PC_PREFIX}
               NO_DEFAULT_PATH
               PATH_SUFFIXES "bin")
elseif(gRPC_ROOT)
  find_library(GRPC_GPR_LIB gpr
               PATHS ${GRPC_ROOT} "${GRPC_ROOT}/Library"
               PATH_SUFFIXES "lib64" "lib" "bin"
               NO_DEFAULT_PATH)
  find_library(GRPC_GRPC_LIB grpc
               PATHS ${GRPC_ROOT} "${GRPC_ROOT}/Library"
               PATH_SUFFIXES "lib64" "lib" "bin"
               NO_DEFAULT_PATH)
  find_library(GRPC_GRPCPP_LIB grpc++
               PATHS ${GRPC_ROOT} "${GRPC_ROOT}/Library"
               PATH_SUFFIXES "lib64" "lib" "bin"
               NO_DEFAULT_PATH)
  find_library(GRPC_ADDRESS_SORTING_LIB address_sorting
               PATHS ${GRPC_ROOT} "${GRPC_ROOT}/Library"
               PATH_SUFFIXES "lib64" "lib" "bin"
               NO_DEFAULT_PATH)
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin NO_DEFAULT_PATH
               PATHS ${GRPC_ROOT} "${GRPC_ROOT}/Library"
               PATH_SUFFIXES "bin")
  find_path(GRPC_INCLUDE_DIR
            NAMES grpc/grpc.h
            PATHS ${GRPC_ROOT} "${GRPC_ROOT}/Library"
            NO_DEFAULT_PATH
            PATH_SUFFIXES "include")
else()
  find_library(GRPC_GPR_LIB gpr PATH_SUFFIXES "lib64" "lib" "bin")
  find_library(GRPC_GRPC_LIB grpc PATH_SUFFIXES "lib64" "lib" "bin")
  find_library(GRPC_GRPCPP_LIB grpc++ PATH_SUFFIXES "lib64" "lib" "bin")
  find_library(GRPC_ADDRESS_SORTING_LIB address_sorting PATH_SUFFIXES "lib64" "lib" "bin")
  find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin PATH_SUFFIXES "bin")
  find_path(GRPC_INCLUDE_DIR NAMES grpc/grpc.h PATH_SUFFIXES "include")
endif()

find_package_handle_standard_args(gRPCAlt
                                  REQUIRED_VARS
                                  GRPC_INCLUDE_DIR
                                  GRPC_GPR_LIB
                                  GRPC_GRPC_LIB
                                  GRPC_GRPCPP_LIB
                                  GRPC_CPP_PLUGIN)

if(gRPCAlt_FOUND)
  add_library(gRPC::gpr UNKNOWN IMPORTED)
  set_target_properties(gRPC::gpr
                        PROPERTIES IMPORTED_LOCATION "${GRPC_GPR_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc UNKNOWN IMPORTED)
  set_target_properties(gRPC::grpc
                        PROPERTIES IMPORTED_LOCATION "${GRPC_GRPC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  add_library(gRPC::grpc++ UNKNOWN IMPORTED)
  set_target_properties(gRPC::grpc++
                        PROPERTIES IMPORTED_LOCATION "${GRPC_GRPCPP_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")

  if(GRPC_ADDRESS_SORTING_LIB)
    # Address sorting is optional and not always requird.
    add_library(gRPC::address_sorting UNKNOWN IMPORTED)
    set_target_properties(gRPC::address_sorting
                          PROPERTIES IMPORTED_LOCATION "${GRPC_ADDRESS_SORTING_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${GRPC_INCLUDE_DIR}")
  endif()

  add_executable(gRPC::grpc_cpp_plugin IMPORTED)
  set_target_properties(gRPC::grpc_cpp_plugin
                        PROPERTIES IMPORTED_LOCATION ${GRPC_CPP_PLUGIN})
endif()
