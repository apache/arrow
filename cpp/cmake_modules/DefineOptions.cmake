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

macro(set_option_category name)
  set(ARROW_OPTION_CATEGORY ${name})
  list(APPEND "ARROW_OPTION_CATEGORIES" ${name})
endmacro()

macro(define_option name description default)
  option(${name} ${description} ${default})
  list(APPEND "ARROW_${ARROW_OPTION_CATEGORY}_OPTION_NAMES" ${name})
  set("${name}_OPTION_DESCRIPTION" ${description})
  set("${name}_OPTION_DEFAULT" ${default})
  set("${name}_OPTION_TYPE" "bool")
endmacro()

function(list_join lst glue out)
  if("${${lst}}" STREQUAL "")
    set(${out} "" PARENT_SCOPE)
    return()
  endif()

  list(GET ${lst} 0 joined)
  list(REMOVE_AT ${lst} 0)
  foreach(item ${${lst}})
    set(joined "${joined}${glue}${item}")
  endforeach()
  set(${out} ${joined} PARENT_SCOPE)
endfunction()

macro(define_option_string name description default)
  set(${name} ${default} CACHE STRING ${description})
  list(APPEND "ARROW_${ARROW_OPTION_CATEGORY}_OPTION_NAMES" ${name})
  set("${name}_OPTION_DESCRIPTION" ${description})
  set("${name}_OPTION_DEFAULT" "\"${default}\"")
  set("${name}_OPTION_TYPE" "string")

  set("${name}_OPTION_ENUM" ${ARGN})
  list_join("${name}_OPTION_ENUM" "|" "${name}_OPTION_ENUM")
  if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
    set_property(CACHE ${name} PROPERTY STRINGS ${ARGN})
  endif()
endmacro()

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  #----------------------------------------------------------------------
  set_option_category("Compile and link")

  define_option_string(ARROW_CXXFLAGS "Compiler flags to append when compiling Arrow" "")

  define_option(ARROW_BUILD_STATIC "Build static libraries" ON)

  define_option(ARROW_BUILD_SHARED "Build shared libraries" ON)

  define_option(ARROW_NO_DEPRECATED_API "Exclude deprecated APIs from build" OFF)

  define_option(ARROW_USE_CCACHE "Use ccache when compiling (if available)" ON)

  define_option(ARROW_USE_LD_GOLD "Use ld.gold for linking on Linux (if available)" OFF)

  # Disable this option to exercise non-SIMD fallbacks
  define_option(ARROW_USE_SIMD "Build with SIMD optimizations" ON)

  define_option(ARROW_ALTIVEC "Build Arrow with Altivec" ON)

  define_option(ARROW_RPATH_ORIGIN "Build Arrow libraries with RATH set to \$ORIGIN" OFF)

  define_option(ARROW_INSTALL_NAME_RPATH
                "Build Arrow libraries with install_name set to @rpath" ON)

  define_option(ARROW_GGDB_DEBUG "Pass -ggdb flag to debug builds" ON)

  #----------------------------------------------------------------------
  set_option_category("Test and benchmark")

  define_option(ARROW_BUILD_EXAMPLES "Build the Arrow examples, default OFF" OFF)

  define_option(ARROW_BUILD_TESTS "Build the Arrow googletest unit tests, default OFF"
                OFF)

  define_option(ARROW_BUILD_INTEGRATION
                "Build the Arrow integration test executables, default OFF" OFF)

  define_option(ARROW_BUILD_BENCHMARKS "Build the Arrow micro benchmarks, default OFF"
                OFF)

  # Reference benchmarks are used to compare to naive implementation, or
  # discover various hardware limits.
  define_option(ARROW_BUILD_BENCHMARKS_REFERENCE
                "Build the Arrow micro reference benchmarks, default OFF." OFF)

  define_option_string(ARROW_TEST_LINKAGE
                       "Linkage of Arrow libraries with unit tests executables."
                       "shared"
                       "shared"
                       "static")

  define_option(ARROW_FUZZING "Build Arrow Fuzzing executables" OFF)

  #----------------------------------------------------------------------
  set_option_category("Lint")

  define_option(ARROW_ONLY_LINT "Only define the lint and check-format targets" OFF)

  define_option(ARROW_VERBOSE_LINT "If off, 'quiet' flags will be passed to linting tools"
                OFF)

  define_option(ARROW_GENERATE_COVERAGE "Build with C++ code coverage enabled" OFF)

  #----------------------------------------------------------------------
  set_option_category("Checks")

  define_option(ARROW_TEST_MEMCHECK "Run the test suite using valgrind --tool=memcheck"
                OFF)

  define_option(ARROW_USE_ASAN "Enable Address Sanitizer checks" OFF)

  define_option(ARROW_USE_TSAN "Enable Thread Sanitizer checks" OFF)

  define_option(ARROW_USE_UBSAN "Enable Undefined Behavior sanitizer checks" OFF)

  #----------------------------------------------------------------------
  set_option_category("Project component")

  define_option(ARROW_COMPUTE "Build the Arrow Compute Modules" ON)

  define_option(ARROW_DATASET "Build the Arrow Dataset Modules" ON)

  define_option(ARROW_FLIGHT
                "Build the Arrow Flight RPC System (requires GRPC, Protocol Buffers)" OFF)

  define_option(ARROW_GANDIVA "Build the Gandiva libraries" OFF)

  define_option(ARROW_PARQUET "Build the Parquet libraries" OFF)

  define_option(ARROW_IPC "Build the Arrow IPC extensions" ON)

  define_option(ARROW_BUILD_UTILITIES "Build Arrow commandline utilities" ON)

  define_option(ARROW_CUDA "Build the Arrow CUDA extensions (requires CUDA toolkit)" OFF)

  define_option(ARROW_ORC "Build the Arrow ORC adapter" OFF)

  define_option(ARROW_JNI "Build the Arrow JNI lib" OFF)

  define_option(ARROW_TENSORFLOW "Build Arrow with TensorFlow support enabled" OFF)

  define_option(ARROW_JEMALLOC "Build the Arrow jemalloc-based allocator" ON)

  define_option(ARROW_HDFS "Build the Arrow HDFS bridge" ON)

  define_option(ARROW_PYTHON "Build the Arrow CPython extensions" OFF)

  define_option(ARROW_HIVESERVER2 "Build the HiveServer2 client and Arrow adapter" OFF)

  define_option(ARROW_PLASMA "Build the plasma object store along with Arrow" OFF)

  define_option(ARROW_PLASMA_JAVA_CLIENT "Build the plasma object store java client" OFF)

  define_option(ARROW_JSON "Build Arrow with JSON support (requires RapidJSON)" ON)

  #----------------------------------------------------------------------
  set_option_category("Thirdparty toolchain")

  # Determine how we will look for dependencies
  # * AUTO: Guess which packaging systems we're running in and pull the
  #   dependencies from there. Build any missing ones through the
  #   ExternalProject setup. This is the default unless the CONDA_PREFIX
  #   environment variable is set, in which case the CONDA method is used
  # * BUNDLED: Build dependencies through CMake's ExternalProject facility. If
  #   you wish to build individual dependencies from source instead of using
  #   one of the other methods, pass -D$NAME_SOURCE=BUNDLED
  # * SYSTEM: Use CMake's find_package and find_library without any custom
  #   paths. If individual packages are on non-default locations, you can pass
  #   $NAME_ROOT arguments to CMake, or set environment variables for the same
  #   with CMake 3.11 and higher.  If your system packages are in a non-default
  #   location, or if you are using a non-standard toolchain, you can also pass
  #   ARROW_PACKAGE_PREFIX to set the *_ROOT variables to look in that
  #   directory
  # * CONDA: Same as system but set all *_ROOT variables to
  #   ENV{CONDA_PREFIX}. If this is run within an active conda environment,
  #   then ENV{CONDA_PREFIX} will be used for dependencies unless
  #   ARROW_DEPENDENCY_SOURCE is set explicitly to one of the other options
  # * BREW: Use SYSTEM but search for select packages with brew.
  if(NOT "$ENV{CONDA_PREFIX}" STREQUAL "")
    set(ARROW_DEPENDENCY_SOURCE_DEFAULT "CONDA")
  else()
    set(ARROW_DEPENDENCY_SOURCE_DEFAULT "AUTO")
  endif()
  define_option_string(ARROW_DEPENDENCY_SOURCE
                       "Method to use for acquiring arrow's build dependencies"
                       "${ARROW_DEPENDENCY_SOURCE_DEFAULT}"
                       "AUTO"
                       "BUNDLED"
                       "SYSTEM"
                       "CONDA"
                       "BREW")

  define_option(ARROW_VERBOSE_THIRDPARTY_BUILD
                "Show output from ExternalProjects rather than just logging to files" OFF)

  define_option(ARROW_BOOST_USE_SHARED "Rely on boost shared libraries where relevant" ON)

  define_option(ARROW_BOOST_VENDORED "Use vendored Boost instead of existing Boost. \
Note that this requires linking Boost statically. \
Deprecated. Use BOOST_SOURCE=BUNDLED instead." OFF)

  define_option(ARROW_PROTOBUF_USE_SHARED
                "Rely on Protocol Buffers shared libraries where relevant" ON)

  define_option(ARROW_GFLAGS_USE_SHARED "Rely on GFlags shared libraries where relevant"
                ON)

  define_option(ARROW_WITH_BACKTRACE "Build with backtrace support" ON)

  define_option(ARROW_USE_GLOG "Build libraries with glog support for pluggable logging"
                ON)

  define_option(ARROW_WITH_BROTLI "Build with Brotli compression" ON)

  define_option(ARROW_WITH_BZ2 "Build with BZ2 compression" OFF)

  define_option(ARROW_WITH_LZ4 "Build with lz4 compression" ON)

  define_option(ARROW_WITH_SNAPPY "Build with Snappy compression" ON)

  define_option(ARROW_WITH_ZLIB "Build with zlib compression" ON)

  if(CMAKE_VERSION VERSION_LESS 3.7)
    set(ARROW_WITH_ZSTD_DEFAULT OFF)
  else()
    # ExternalProject_Add(SOURCE_SUBDIR) is available since CMake 3.7.
    set(ARROW_WITH_ZSTD_DEFAULT ON)
  endif()
  define_option(ARROW_WITH_ZSTD "Build with zstd compression" ${ARROW_WITH_ZSTD_DEFAULT})

  #----------------------------------------------------------------------
  if(MSVC)
    set_option_category("MSVC")

    define_option(MSVC_LINK_VERBOSE
                  "Pass verbose linking options when linking libraries and executables"
                  OFF)

    define_option(ARROW_USE_CLCACHE "Use clcache if available" ON)

    define_option_string(BROTLI_MSVC_STATIC_LIB_SUFFIX
                         "Brotli static lib suffix used on Windows with MSVC" "-static")

    define_option_string(PROTOBUF_MSVC_STATIC_LIB_SUFFIX
                         "Protobuf static lib suffix used on Windows with MSVC" "")

    define_option_string(RE2_MSVC_STATIC_LIB_SUFFIX
                         "re2 static lib suffix used on Windows with MSVC" "_static")

    define_option_string(SNAPPY_MSVC_STATIC_LIB_SUFFIX
                         "Snappy static lib suffix used on Windows with MSVC" "_static")

    define_option_string(LZ4_MSVC_STATIC_LIB_SUFFIX
                         "Lz4 static lib suffix used on Windows with MSVC" "_static")

    define_option_string(ZSTD_MSVC_STATIC_LIB_SUFFIX
                         "ZStd static lib suffix used on Windows with MSVC" "_static")

    define_option(ARROW_USE_STATIC_CRT "Build Arrow with statically linked CRT" OFF)
  endif()

  #----------------------------------------------------------------------
  set_option_category("Parquet")

  define_option(PARQUET_MINIMAL_DEPENDENCY
                "Depend only on Thirdparty headers to build libparquet. \
Always OFF if building binaries" OFF)

  define_option(
    PARQUET_BUILD_EXECUTABLES
    "Build the Parquet executable CLI tools. Requires static libraries to be built." OFF)

  define_option(PARQUET_BUILD_EXAMPLES
                "Build the Parquet examples. Requires static libraries to be built." OFF)

  define_option(PARQUET_REQUIRE_ENCRYPTION
                "Build support for encryption. Fail if OpenSSL is not found" OFF)

  #----------------------------------------------------------------------
  set_option_category("Gandiva")

  define_option(ARROW_GANDIVA_JAVA "Build the Gandiva JNI wrappers" OFF)

  # ARROW-3860: Temporary workaround
  define_option(
    ARROW_GANDIVA_STATIC_LIBSTDCPP
    "Include -static-libstdc++ -static-libgcc when linking with Gandiva static libraries"
    OFF)

  define_option_string(ARROW_GANDIVA_PC_CXX_FLAGS
                       "Compiler flags to append when pre-compiling Gandiva operations"
                       "")

  #----------------------------------------------------------------------
  set_option_category("Advanced developer")

  define_option(ARROW_EXTRA_ERROR_CONTEXT
                "Compile with extra error context (line numbers, code)" OFF)

  define_option(ARROW_OPTIONAL_INSTALL
                "If enabled install ONLY targets that have already been built. Please be \
advised that if this is enabled 'install' will fail silently on components \
that have not been built" OFF)

  option(ARROW_BUILD_CONFIG_SUMMARY_JSON "Summarize build configuration in a JSON file"
         ON)
endif()

macro(config_summary)
  message(STATUS "---------------------------------------------------------------------")
  message(STATUS "Arrow version:                                 ${ARROW_VERSION}")
  message(STATUS)
  message(STATUS "Build configuration summary:")

  message(STATUS "  Generator: ${CMAKE_GENERATOR}")
  message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
  message(STATUS "  Source directory: ${CMAKE_CURRENT_SOURCE_DIR}")
  message(STATUS "  Install prefix: ${CMAKE_INSTALL_PREFIX}")
  if(${CMAKE_EXPORT_COMPILE_COMMANDS})
    message(
      STATUS "  Compile commands: ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json")
  endif()

  if(${ARROW_BUILD_CONFIG_SUMMARY_JSON})
    set(summary "${CMAKE_CURRENT_BINARY_DIR}/cmake_summary.json")
    message(STATUS "  Outputting build configuration summary to ${summary}")
    file(WRITE ${summary} "{\n")
  endif()

  foreach(category ${ARROW_OPTION_CATEGORIES})

    message(STATUS)
    message(STATUS "${category} options:")

    set(option_names ${ARROW_${category}_OPTION_NAMES})

    set(max_value_length 0)
    foreach(name ${option_names})
      string(LENGTH "\"${${name}}\"" value_length)
      if(${max_value_length} LESS ${value_length})
        set(max_value_length ${value_length})
      endif()
    endforeach()

    foreach(name ${option_names})
      if("${${name}_OPTION_TYPE}" STREQUAL "string")
        set(value "\"${${name}}\"")
      else()
        set(value "${${name}}")
      endif()

      set(default ${${name}_OPTION_DEFAULT})
      set(description ${${name}_OPTION_DESCRIPTION})
      string(LENGTH ${description} description_length)
      if(${description_length} LESS 70)
        string(
          SUBSTRING
            "                                                                     "
            ${description_length} -1 description_padding)
      else()
        set(description_padding "
                                                                           ")
      endif()

      set(comment "[${name}]")

      if("${value}" STREQUAL "${default}")
        set(comment "[default] ${comment}")
      endif()

      if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
        set(comment "${comment} [${${name}_OPTION_ENUM}]")
      endif()

      string(
        SUBSTRING "${value}                                                             "
                  0 ${max_value_length} value)

      message(STATUS "  ${description} ${description_padding} ${value} ${comment}")
    endforeach()

    if(${ARROW_BUILD_CONFIG_SUMMARY_JSON})
      foreach(name ${option_names})
        file(APPEND ${summary} "\"${name}\": \"${${name}}\",\n")
      endforeach()
    endif()

  endforeach()

  if(${ARROW_BUILD_CONFIG_SUMMARY_JSON})
    file(APPEND ${summary} "\"generator\": \"${CMAKE_GENERATOR}\",\n")
    file(APPEND ${summary} "\"build_type\": \"${CMAKE_BUILD_TYPE}\",\n")
    file(APPEND ${summary} "\"source_dir\": \"${CMAKE_CURRENT_SOURCE_DIR}\",\n")
    if(${CMAKE_EXPORT_COMPILE_COMMANDS})
      file(APPEND ${summary} "\"compile_commands\": "
                             "\"${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json\",\n")
    endif()
    file(APPEND ${summary} "\"install_prefix\": \"${CMAKE_INSTALL_PREFIX}\",\n")
    file(APPEND ${summary} "\"arrow_version\": \"${ARROW_VERSION}\"\n")
    file(APPEND ${summary} "}\n")
  endif()

endmacro()
