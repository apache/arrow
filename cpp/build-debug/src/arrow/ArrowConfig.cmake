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
#
# This config sets the following variables in your project::
#
#   ARROW_FULL_SO_VERSION - full shared library version of the found Arrow
#   ARROW_SO_VERSION - shared library version of the found Arrow
#   ARROW_VERSION - version of the found Arrow
#   ARROW_* - options used when the found Arrow is build such as ARROW_COMPUTE
#   Arrow_FOUND - true if Arrow found on the system
#
# This config sets the following targets in your project::
#
#   Arrow::arrow_shared - for linked as shared library if shared library is built
#   Arrow::arrow_static - for linked as static library if static library is built


####### Expanded from @PACKAGE_INIT@ by configure_package_config_file() #######
####### Any changes to this file will be overwritten by the next CMake run ####
####### The input file was ArrowConfig.cmake.in                            ########

get_filename_component(PACKAGE_PREFIX_DIR "${CMAKE_CURRENT_LIST_DIR}/../../../" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

macro(check_required_components _NAME)
  foreach(comp ${${_NAME}_FIND_COMPONENTS})
    if(NOT ${_NAME}_${comp}_FOUND)
      if(${_NAME}_FIND_REQUIRED_${comp})
        set(${_NAME}_FOUND FALSE)
      endif()
    endif()
  endforeach()
endmacro()

####################################################################################

set(ARROW_VERSION "15.0.0-SNAPSHOT")
set(ARROW_SO_VERSION "1500")
set(ARROW_FULL_SO_VERSION "1500.0.0")

set(ARROW_BUNDLED_STATIC_LIBS "jemalloc::jemalloc;re2::re2;utf8proc::utf8proc")
set(ARROW_INCLUDE_PATH_SUFFIXES "include;Library;Library/include")
set(ARROW_LIBRARY_PATH_SUFFIXES ";lib/;lib64;lib32;lib;bin;Library;Library/lib;Library/bin")
set(ARROW_SYSTEM_DEPENDENCIES "")

include("${CMAKE_CURRENT_LIST_DIR}/ArrowOptions.cmake")

macro(arrow_find_dependencies dependencies)
  if(DEFINED CMAKE_MODULE_PATH)
    set(ARROW_CMAKE_MODULE_PATH_OLD ${CMAKE_MODULE_PATH})
  else()
    unset(ARROW_CMAKE_MODULE_PATH_OLD)
  endif()
  set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")

  foreach(dependency ${dependencies})
    set(ARROW_OPENSSL_HOMEBREW_MAKE_DETECTABLE FALSE)
    if(${dependency} STREQUAL "OpenSSL" AND NOT OPENSSL_ROOT_DIR)
      find_program(ARROW_BREW brew)
      if(ARROW_BREW)
        set(ARROW_OPENSSL_ROOT_DIR_ORIGINAL ${OPENSSL_ROOT_DIR})
        execute_process(COMMAND ${ARROW_BREW} --prefix "openssl@1.1"
                        OUTPUT_VARIABLE OPENSSL11_BREW_PREFIX
                        OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(OPENSSL11_BREW_PREFIX)
          set(OPENSSL_ROOT_DIR ${OPENSSL11_BREW_PREFIX})
          set(ARROW_OPENSSL_HOMEBREW_MAKE_DETECTABLE TRUE)
        else()
          execute_process(COMMAND ${ARROW_BREW} --prefix "openssl"
                          OUTPUT_VARIABLE OPENSSL_BREW_PREFIX
                          OUTPUT_STRIP_TRAILING_WHITESPACE)
          if(OPENSSL_BREW_PREFIX)
            set(OPENSSL_ROOT_DIR ${OPENSSL_BREW_PREFIX})
            set(ARROW_OPENSSL_HOMEBREW_MAKE_DETECTABLE TRUE)
          endif()
        endif()
      endif()
    endif()
    find_dependency(${dependency})
    if(ARROW_OPENSSL_HOMEBREW_MAKE_DETECTABLE)
      set(OPENSSL_ROOT_DIR ${ARROW_OPENSSL_ROOT_DIR_ORIGINAL})
    endif()
  endforeach()

  if(DEFINED ARROW_CMAKE_MODULE_PATH_OLD)
    set(CMAKE_MODULE_PATH ${ARROW_CMAKE_MODULE_PATH_OLD})
    unset(ARROW_CMAKE_MODULE_PATH_OLD)
  else()
    unset(CMAKE_MODULE_PATH)
  endif()
endmacro()

if(ARROW_BUILD_STATIC)
  include(CMakeFindDependencyMacro)

  if(ARROW_ENABLE_THREADING)
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
    find_dependency(Threads)
  endif()

  arrow_find_dependencies("${ARROW_SYSTEM_DEPENDENCIES}")
endif()

include("${CMAKE_CURRENT_LIST_DIR}/ArrowTargets.cmake")

if(TARGET Arrow::arrow_static AND NOT TARGET Arrow::arrow_bundled_dependencies)
  add_library(Arrow::arrow_bundled_dependencies STATIC IMPORTED)
  get_target_property(arrow_static_configurations Arrow::arrow_static
                      IMPORTED_CONFIGURATIONS)
  foreach(CONFIGURATION ${arrow_static_configurations})
    string(TOUPPER "${CONFIGURATION}" CONFIGURATION)
    get_target_property(arrow_static_location Arrow::arrow_static
                        LOCATION_${CONFIGURATION})
    get_filename_component(arrow_lib_dir "${arrow_static_location}" DIRECTORY)
    set_property(TARGET Arrow::arrow_bundled_dependencies
                 APPEND
                 PROPERTY IMPORTED_CONFIGURATIONS ${CONFIGURATION})
    set_target_properties(Arrow::arrow_bundled_dependencies
                          PROPERTIES IMPORTED_LOCATION_${CONFIGURATION}
                                     "${arrow_lib_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
  endforeach()

  # CMP0057: Support new if() IN_LIST operator.
  # https://cmake.org/cmake/help/latest/policy/CMP0057.html
  cmake_policy(PUSH)
  cmake_policy(SET CMP0057 NEW)
  if("AWS::aws-c-common" IN_LIST ARROW_BUNDLED_STATIC_LIBS)
    if(APPLE)
      find_library(CORE_FOUNDATION CoreFoundation)
      target_link_libraries(Arrow::arrow_bundled_dependencies
                            INTERFACE ${CORE_FOUNDATION})
      find_library(SECURITY Security)
      target_link_libraries(Arrow::arrow_bundled_dependencies INTERFACE ${SECURITY})
    elseif(WIN32)
      target_link_libraries(Arrow::arrow_bundled_dependencies
                            INTERFACE "winhttp.lib"
                                      "bcrypt.lib"
                                      "wininet.lib"
                                      "userenv.lib"
                                      "version.lib"
                                      "ncrypt.lib"
                                      "Secur32.lib"
                                      "Shlwapi.lib")
    endif()
  endif()
  cmake_policy(POP)
endif()

macro(arrow_keep_backward_compatibility namespace target_base_name)
  string(TOUPPER ${target_base_name} target_base_name_upper)

  if(NOT CMAKE_VERSION VERSION_LESS 3.18)
    if(TARGET ${namespace}::${target_base_name}_shared AND NOT TARGET
                                                           ${target_base_name}_shared)
      add_library(${target_base_name}_shared ALIAS
                  ${namespace}::${target_base_name}_shared)
    endif()
    if(TARGET ${namespace}::${target_base_name}_static AND NOT TARGET
                                                           ${target_base_name}_static)
      add_library(${target_base_name}_static ALIAS
                  ${namespace}::${target_base_name}_static)
    endif()
  endif()

  if(TARGET ${namespace}::${target_base_name}_shared)
    get_target_property(${target_base_name_upper}_INCLUDE_DIR
                        ${namespace}::${target_base_name}_shared
                        INTERFACE_INCLUDE_DIRECTORIES)
  else()
    get_target_property(${target_base_name_upper}_INCLUDE_DIR
                        ${namespace}::${target_base_name}_static
                        INTERFACE_INCLUDE_DIRECTORIES)
  endif()

  foreach(BUILD_TYPE_SUFFIX
          "_RELEASE"
          "_RELWITHDEBINFO"
          "_MINSIZEREL"
          "_DEBUG"
          "")
    if(TARGET ${namespace}::${target_base_name}_shared)
      if(NOT ${target_base_name_upper}_SHARED_LIB)
        get_target_property(${target_base_name_upper}_SHARED_LIB
                            ${namespace}::${target_base_name}_shared
                            IMPORTED_LOCATION${BUILD_TYPE_SUFFIX})
      endif()
      if(NOT ${target_base_name_upper}_IMPORT_LIB)
        get_target_property(${target_base_name_upper}_IMPORT_LIB
                            ${namespace}::${target_base_name}_shared
                            IMPORTED_IMPLIB${BUILD_TYPE_SUFFIX})
      endif()
    endif()

    if(TARGET ${namespace}::${target_base_name}_static)
      if(NOT ${target_base_name_upper}_STATIC_LIB)
        get_target_property(${target_base_name_upper}_STATIC_LIB
                            ${namespace}::${target_base_name}_static
                            IMPORTED_LOCATION${BUILD_TYPE_SUFFIX})
      endif()
    endif()
  endforeach()
endmacro()

arrow_keep_backward_compatibility(Arrow arrow)

check_required_components(Arrow)

macro(arrow_show_details package_name variable_prefix)
  if(NOT ${package_name}_FIND_QUIETLY AND NOT ${package_name}_SHOWED_DETAILS)
    message(STATUS "${package_name} version: ${${package_name}_VERSION}")
    message(STATUS "Found the ${package_name} shared library: ${${variable_prefix}_SHARED_LIB}"
    )
    message(STATUS "Found the ${package_name} import library: ${${variable_prefix}_IMPORT_LIB}"
    )
    message(STATUS "Found the ${package_name} static library: ${${variable_prefix}_STATIC_LIB}"
    )
    set(${package_name}_SHOWED_DETAILS TRUE)
  endif()
endmacro()

arrow_show_details(Arrow ARROW)
