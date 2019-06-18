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
#
# Tries to find Brotli headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Brotli)

if(BROTLI_ROOT)
  find_library(
    BROTLI_COMMON_LIBRARY
    NAMES brotlicommon
          ${CMAKE_SHARED_LIBRARY_PREFIX}brotlicommon${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon${CMAKE_STATIC_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon-static${CMAKE_STATIC_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${BROTLI_ROOT}
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
    NO_DEFAULT_PATH)
  find_library(
    BROTLI_ENC_LIBRARY
    NAMES brotlienc
          ${CMAKE_SHARED_LIBRARY_PREFIX}brotlienc${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc${CMAKE_STATIC_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc-static${CMAKE_STATIC_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${BROTLI_ROOT}
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
    NO_DEFAULT_PATH)
  find_library(
    BROTLI_DEC_LIBRARY
    NAMES brotlidec
          ${CMAKE_SHARED_LIBRARY_PREFIX}brotlidec${CMAKE_SHARED_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec${CMAKE_STATIC_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec-static${CMAKE_STATIC_LIBRARY_SUFFIX}
          ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec_static${CMAKE_STATIC_LIBRARY_SUFFIX}
    PATHS ${BROTLI_ROOT}
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
    NO_DEFAULT_PATH)
  find_path(BROTLI_INCLUDE_DIR
            NAMES brotli/decode.h
            PATHS ${BROTLI_ROOT}
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES}
            NO_DEFAULT_PATH)
else()
  pkg_check_modules(BROTLI_PC libbrotlicommon libbrotlienc libbrotlidec)
  if(BROTLI_PC_FOUND)
    set(BROTLI_INCLUDE_DIR "${BROTLI_PC_libbrotlicommon_INCLUDEDIR}")

    # Some systems (e.g. Fedora) don't fill Brotli_LIBRARY_DIRS, so add the other dirs here.
    list(APPEND BROTLI_PC_LIBRARY_DIRS "${BROTLI_PC_libbrotlicommon_LIBDIR}")
    list(APPEND BROTLI_PC_LIBRARY_DIRS "${BROTLI_PC_libbrotlienc_LIBDIR}")
    list(APPEND BROTLI_PC_LIBRARY_DIRS "${BROTLI_PC_libbrotlidec_LIBDIR}")

    find_library(BROTLI_COMMON_LIBRARY brotlicommon
                 PATHS ${BROTLI_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(BROTLI_ENC_LIBRARY brotlienc
                 PATHS ${BROTLI_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_library(BROTLI_DEC_LIBRARY brotlidec
                 PATHS ${BROTLI_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
  else()
    find_library(
      BROTLI_COMMON_LIBRARY
      NAMES
        brotlicommon
        ${CMAKE_SHARED_LIBRARY_PREFIX}brotlicommon${CMAKE_SHARED_LIBRARY_SUFFIX}
        ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon-static${CMAKE_STATIC_LIBRARY_SUFFIX}
        ${CMAKE_STATIC_LIBRARY_PREFIX}brotlicommon_static${CMAKE_STATIC_LIBRARY_SUFFIX}
      PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
    find_library(
      BROTLI_ENC_LIBRARY
      NAMES brotlienc
            ${CMAKE_SHARED_LIBRARY_PREFIX}brotlienc${CMAKE_SHARED_LIBRARY_SUFFIX}
            ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc${CMAKE_STATIC_LIBRARY_SUFFIX}
            ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc-static${CMAKE_STATIC_LIBRARY_SUFFIX}
            ${CMAKE_STATIC_LIBRARY_PREFIX}brotlienc_static${CMAKE_STATIC_LIBRARY_SUFFIX}
      PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
    find_library(
      BROTLI_DEC_LIBRARY
      NAMES brotlidec
            ${CMAKE_SHARED_LIBRARY_PREFIX}brotlidec${CMAKE_SHARED_LIBRARY_SUFFIX}
            ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec${CMAKE_STATIC_LIBRARY_SUFFIX}
            ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec-static${CMAKE_STATIC_LIBRARY_SUFFIX}
            ${CMAKE_STATIC_LIBRARY_PREFIX}brotlidec_static${CMAKE_STATIC_LIBRARY_SUFFIX}
      PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
    find_path(BROTLI_INCLUDE_DIR
              NAMES brotli/decode.h
              PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
  endif()
endif()

find_package_handle_standard_args(Brotli
                                  REQUIRED_VARS
                                  BROTLI_COMMON_LIBRARY
                                  BROTLI_ENC_LIBRARY
                                  BROTLI_DEC_LIBRARY
                                  BROTLI_INCLUDE_DIR)
if(Brotli_FOUND OR BROTLI_FOUND)
  set(Brotli_FOUND TRUE)
  add_library(Brotli::brotlicommon UNKNOWN IMPORTED)
  set_target_properties(Brotli::brotlicommon
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_COMMON_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
  add_library(Brotli::brotlienc UNKNOWN IMPORTED)
  set_target_properties(Brotli::brotlienc
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_ENC_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
  add_library(Brotli::brotlidec UNKNOWN IMPORTED)
  set_target_properties(Brotli::brotlidec
                        PROPERTIES IMPORTED_LOCATION "${BROTLI_DEC_LIBRARY}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${BROTLI_INCLUDE_DIR}")
endif()
