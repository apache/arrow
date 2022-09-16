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

function(check_description_length name description)
  foreach(description_line ${description})
    string(LENGTH ${description_line} line_length)
    if(${line_length} GREATER 80)
      message(FATAL_ERROR "description for ${name} contained a\n\
        line ${line_length} characters long!\n\
        (max is 80). Split it into more lines with semicolons")
    endif()
  endforeach()
endfunction()

function(list_join lst glue out)
  if("${${lst}}" STREQUAL "")
    set(${out}
        ""
        PARENT_SCOPE)
    return()
  endif()

  list(GET ${lst} 0 joined)
  list(REMOVE_AT ${lst} 0)
  foreach(item ${${lst}})
    set(joined "${joined}${glue}${item}")
  endforeach()
  set(${out}
      ${joined}
      PARENT_SCOPE)
endfunction()

macro(define_option name description default)
  set(options)
  set(one_value_args)
  set(multi_value_args DEPENDS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  check_description_length(${name} ${description})
  list_join(description "\n" multiline_description)

  option(${name} "${multiline_description}" ${default})

  list(APPEND "ARROW_${ARROW_OPTION_CATEGORY}_OPTION_NAMES" ${name})
  set("${name}_OPTION_DESCRIPTION" ${description})
  set("${name}_OPTION_DEFAULT" ${default})
  set("${name}_OPTION_TYPE" "bool")
  set("${name}_OPTION_DEPENDS" ${ARG_DEPENDS})
endmacro()

macro(define_option_string name description default)
  check_description_length(${name} ${description})
  list_join(description "\n" multiline_description)

  set(${name}
      ${default}
      CACHE STRING "${multiline_description}")

  list(APPEND "ARROW_${ARROW_OPTION_CATEGORY}_OPTION_NAMES" ${name})
  set("${name}_OPTION_DESCRIPTION" ${description})
  set("${name}_OPTION_DEFAULT" "\"${default}\"")
  set("${name}_OPTION_TYPE" "string")
  set("${name}_OPTION_POSSIBLE_VALUES" ${ARGN})

  list_join("${name}_OPTION_POSSIBLE_VALUES" "|" "${name}_OPTION_ENUM")
  if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
    set_property(CACHE ${name} PROPERTY STRINGS "${name}_OPTION_POSSIBLE_VALUES")
  endif()
endmacro()

# Topological sort by Tarjan's algorithm.
set(ARROW_BOOL_OPTION_DEPENDENCIES_TSORTED)
macro(tsort_bool_option_dependencies_visit option_name)
  if("${${option_name}_TSORT_STATUS}" STREQUAL "VISITING")
    message(FATAL_ERROR "Cyclic option dependency is detected: ${option_name}")
  elseif("${${option_name}_TSORT_STATUS}" STREQUAL "")
    set(${option_name}_TSORT_STATUS "VISITING")
    foreach(needed_option_name ${${option_name}_OPTION_DEPENDS})
      tsort_bool_option_dependencies_visit(${needed_option_name})
    endforeach()
    set(${option_name}_TSORT_STATUS "VISITED")
    list(INSERT ARROW_BOOL_OPTION_DEPENDENCIES_TSORTED 0 ${option_name})
  endif()
endmacro()
macro(tsort_bool_option_dependencies)
  foreach(category ${ARROW_OPTION_CATEGORIES})
    foreach(option_name ${ARROW_${category}_OPTION_NAMES})
      if("${${option_name}_OPTION_TYPE}" STREQUAL "bool")
        if("${${option_name}_TSORT_STATUS}" STREQUAL "")
          tsort_bool_option_dependencies_visit(${option_name})
        endif()
      endif()
    endforeach()
  endforeach()
endmacro()

macro(resolve_option_dependencies)
  if(MSVC_TOOLCHAIN)
    # Plasma using glog is not fully tested on windows.
    set(ARROW_USE_GLOG OFF)
  endif()

  tsort_bool_option_dependencies()
  foreach(option_name ${ARROW_BOOL_OPTION_DEPENDENCIES_TSORTED})
    if(${${option_name}})
      foreach(depended_option_name ${${option_name}_OPTION_DEPENDS})
        set(${depended_option_name} ON)
      endforeach()
    endif()
  endforeach()
endmacro()

# Top level cmake dir
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  set(ARROW_DEFINE_OPTIONS_DEFAULT ON)
else()
  set(ARROW_DEFINE_OPTIONS_DEFAULT OFF)
endif()
option(ARROW_DEFINE_OPTIONS "Define Arrow options" ${ARROW_DEFINE_OPTIONS_DEFAULT})
if(ARROW_DEFINE_OPTIONS)
  #----------------------------------------------------------------------
  set_option_category("Compile and link")

  define_option_string(ARROW_CXXFLAGS "Compiler flags to append when compiling Arrow" "")

  define_option(ARROW_BUILD_STATIC "Build static libraries" ON)

  define_option(ARROW_BUILD_SHARED "Build shared libraries" ON)

  define_option_string(ARROW_PACKAGE_KIND
                       "Arbitrary string that identifies the kind of package;\
(for informational purposes)" "")

  define_option_string(ARROW_GIT_ID "The Arrow git commit id (if any)" "")

  define_option_string(ARROW_GIT_DESCRIPTION "The Arrow git commit description (if any)"
                       "")

  define_option(ARROW_NO_DEPRECATED_API "Exclude deprecated APIs from build" OFF)

  define_option(ARROW_POSITION_INDEPENDENT_CODE
                "Whether to create position-independent target" ON)

  define_option(ARROW_USE_CCACHE "Use ccache when compiling (if available)" ON)

  define_option(ARROW_USE_SCCACHE "Use sccache when compiling (if available),;\
takes precedence over ccache if a storage backend is configured" ON)

  define_option(ARROW_USE_LD_GOLD "Use ld.gold for linking on Linux (if available)" OFF)

  define_option(ARROW_USE_PRECOMPILED_HEADERS "Use precompiled headers when compiling"
                OFF)

  define_option_string(ARROW_SIMD_LEVEL
                       "Compile-time SIMD optimization level"
                       "DEFAULT" # default to SSE4_2 on x86, NEON on Arm, NONE otherwise
                       "NONE"
                       "SSE4_2"
                       "AVX2"
                       "AVX512"
                       "NEON"
                       "SVE" # size agnostic SVE
                       "SVE128" # fixed size SVE
                       "SVE256" # "
                       "SVE512" # "
                       "DEFAULT")

  define_option_string(ARROW_RUNTIME_SIMD_LEVEL
                       "Max runtime SIMD optimization level"
                       "MAX" # default to max supported by compiler
                       "NONE"
                       "SSE4_2"
                       "AVX2"
                       "AVX512"
                       "MAX")

  define_option(ARROW_ALTIVEC "Build with Altivec if compiler has support" ON)

  define_option(ARROW_RPATH_ORIGIN "Build Arrow libraries with RATH set to \$ORIGIN" OFF)

  define_option(ARROW_INSTALL_NAME_RPATH
                "Build Arrow libraries with install_name set to @rpath" ON)

  define_option(ARROW_GGDB_DEBUG "Pass -ggdb flag to debug builds" ON)

  define_option(ARROW_WITH_MUSL "Whether the system libc is musl or not" OFF)

  #----------------------------------------------------------------------
  set_option_category("Test and benchmark")

  define_option(ARROW_BUILD_EXAMPLES "Build the Arrow examples" OFF)

  define_option(ARROW_BUILD_TESTS
                "Build the Arrow googletest unit tests"
                OFF
                DEPENDS
                ARROW_IPC
                ARROW_TESTING)

  define_option(ARROW_ENABLE_TIMING_TESTS "Enable timing-sensitive tests" ON)

  define_option(ARROW_BUILD_INTEGRATION
                "Build the Arrow integration test executables"
                OFF
                DEPENDS
                ARROW_TESTING)

  define_option(ARROW_BUILD_BENCHMARKS
                "Build the Arrow micro benchmarks"
                OFF
                DEPENDS
                ARROW_IPC
                ARROW_TESTING)

  # Reference benchmarks are used to compare to naive implementation, or
  # discover various hardware limits.
  define_option(ARROW_BUILD_BENCHMARKS_REFERENCE
                "Build the Arrow micro reference benchmarks" OFF)

  define_option(ARROW_BUILD_OPENMP_BENCHMARKS
                "Build the Arrow benchmarks that rely on OpenMP" OFF)

  define_option(ARROW_BUILD_DETAILED_BENCHMARKS
                "Build benchmarks that do a longer exploration of performance" OFF)

  if(ARROW_BUILD_SHARED)
    set(ARROW_TEST_LINKAGE_DEFAULT "shared")
  else()
    set(ARROW_TEST_LINKAGE_DEFAULT "static")
  endif()

  define_option_string(ARROW_TEST_LINKAGE
                       "Linkage of Arrow libraries with unit tests executables."
                       "${ARROW_TEST_LINKAGE_DEFAULT}"
                       "shared"
                       "static")

  define_option(ARROW_FUZZING
                "Build Arrow Fuzzing executables"
                OFF
                DEPENDS
                ARROW_TESTING)

  define_option(ARROW_LARGE_MEMORY_TESTS "Enable unit tests which use large memory" OFF)

  #----------------------------------------------------------------------
  set_option_category("Lint")

  define_option(ARROW_ONLY_LINT "Only define the lint and check-format targets" OFF)

  define_option(ARROW_VERBOSE_LINT
                "If off, 'quiet' flags will be passed to linting tools" OFF)

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

  define_option(ARROW_BUILD_UTILITIES "Build Arrow commandline utilities" OFF)

  define_option(ARROW_COMPUTE "Build the Arrow Compute Modules" OFF)

  define_option(ARROW_CSV "Build the Arrow CSV Parser Module" OFF)

  define_option(ARROW_CUDA
                "Build the Arrow CUDA extensions (requires CUDA toolkit)"
                OFF
                DEPENDS
                ARROW_IPC)

  define_option(ARROW_DATASET
                "Build the Arrow Dataset Modules"
                OFF
                DEPENDS
                ARROW_COMPUTE
                ARROW_FILESYSTEM)

  define_option(ARROW_FILESYSTEM "Build the Arrow Filesystem Layer" OFF)

  define_option(ARROW_FLIGHT
                "Build the Arrow Flight RPC System (requires GRPC, Protocol Buffers)"
                OFF
                DEPENDS
                ARROW_IPC)

  define_option(ARROW_FLIGHT_SQL
                "Build the Arrow Flight SQL extension"
                OFF
                DEPENDS
                ARROW_FLIGHT)

  define_option(ARROW_GANDIVA
                "Build the Gandiva libraries"
                OFF
                DEPENDS
                ARROW_WITH_RE2
                ARROW_WITH_UTF8PROC)

  define_option(ARROW_GCS
                "Build Arrow with GCS support (requires the GCloud SDK for C++)" OFF)

  define_option(ARROW_HDFS "Build the Arrow HDFS bridge" OFF)

  define_option(ARROW_IPC "Build the Arrow IPC extensions" ON)

  set(ARROW_JEMALLOC_DESCRIPTION "Build the Arrow jemalloc-based allocator")
  if(WIN32 OR "${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
    # jemalloc is not supported on Windows.
    #
    # jemalloc is the default malloc implementation on FreeBSD and can't
    # be built with --disable-libdl on FreeBSD. Because lazy-lock feature
    # is required on FreeBSD. Lazy-lock feature requires libdl.
    define_option(ARROW_JEMALLOC ${ARROW_JEMALLOC_DESCRIPTION} OFF)
  else()
    define_option(ARROW_JEMALLOC ${ARROW_JEMALLOC_DESCRIPTION} ON)
  endif()

  define_option(ARROW_JSON "Build Arrow with JSON support (requires RapidJSON)" OFF)

  define_option(ARROW_MIMALLOC "Build the Arrow mimalloc-based allocator" OFF)

  define_option(ARROW_PARQUET
                "Build the Parquet libraries"
                OFF
                DEPENDS
                ARROW_COMPUTE
                ARROW_IPC)

  define_option(ARROW_ORC
                "Build the Arrow ORC adapter"
                OFF
                DEPENDS
                ARROW_WITH_LZ4
                ARROW_WITH_SNAPPY
                ARROW_WITH_ZLIB
                ARROW_WITH_ZSTD)

  define_option(ARROW_PLASMA "Build the plasma object store along with Arrow" OFF)

  define_option(ENABLE_QPL "Enable Intel® Query Processing Library" OFF)

  define_option(ARROW_PYTHON
                "Build some components needed by PyArrow.;\
(This is a deprecated option. Use CMake presets instead.)"
                OFF
                DEPENDS
                ARROW_COMPUTE
                ARROW_CSV
                ARROW_DATASET
                ARROW_FILESYSTEM
                ARROW_HDFS
                ARROW_JSON)

  define_option(ARROW_S3 "Build Arrow with S3 support (requires the AWS SDK for C++)" OFF)

  define_option(ARROW_SKYHOOK
                "Build the Skyhook libraries"
                OFF
                DEPENDS
                ARROW_DATASET
                ARROW_PARQUET
                ARROW_WITH_LZ4
                ARROW_WITH_SNAPPY)

  define_option(ARROW_SUBSTRAIT
                "Build the Arrow Substrait Consumer Module"
                OFF
                DEPENDS
                ARROW_DATASET
                ARROW_IPC
                ARROW_PARQUET)

  define_option(ARROW_TENSORFLOW "Build Arrow with TensorFlow support enabled" OFF)

  define_option(ARROW_TESTING
                "Build the Arrow testing libraries"
                OFF
                DEPENDS
                ARROW_JSON)

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
  # * CONDA: Same as SYSTEM but set all *_ROOT variables to
  #   ENV{CONDA_PREFIX}. If this is run within an active conda environment,
  #   then ENV{CONDA_PREFIX} will be used for dependencies unless
  #   ARROW_DEPENDENCY_SOURCE is set explicitly to one of the other options
  # * VCPKG: Searches for dependencies installed by vcpkg.
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
                       "VCPKG"
                       "BREW")

  define_option(ARROW_VERBOSE_THIRDPARTY_BUILD
                "Show output from ExternalProjects rather than just logging to files" OFF)

  define_option(ARROW_DEPENDENCY_USE_SHARED "Link to shared libraries" ON)

  define_option(ARROW_BOOST_USE_SHARED "Rely on Boost shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_BROTLI_USE_SHARED "Rely on Brotli shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_BZ2_USE_SHARED "Rely on Bz2 shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_GFLAGS_USE_SHARED "Rely on GFlags shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_GRPC_USE_SHARED "Rely on gRPC shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_JEMALLOC_USE_SHARED
                "Rely on jemalloc shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_LZ4_USE_SHARED "Rely on lz4 shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_OPENSSL_USE_SHARED
                "Rely on OpenSSL shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_PROTOBUF_USE_SHARED
                "Rely on Protocol Buffers shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_SNAPPY_USE_SHARED "Rely on snappy shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  if(WIN32)
    # It seems that Thrift doesn't support DLL well yet.
    # MSYS2, conda-forge and vcpkg don't build shared library.
    set(ARROW_THRIFT_USE_SHARED_DEFAULT OFF)
  else()
    set(ARROW_THRIFT_USE_SHARED_DEFAULT ${ARROW_DEPENDENCY_USE_SHARED})
  endif()
  define_option(ARROW_THRIFT_USE_SHARED "Rely on thrift shared libraries where relevant"
                ${ARROW_THRIFT_USE_SHARED_DEFAULT})

  define_option(ARROW_UTF8PROC_USE_SHARED
                "Rely on utf8proc shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_ZSTD_USE_SHARED "Rely on zstd shared libraries where relevant"
                ${ARROW_DEPENDENCY_USE_SHARED})

  define_option(ARROW_USE_GLOG "Build libraries with glog support for pluggable logging"
                OFF)

  define_option(ARROW_WITH_BACKTRACE "Build with backtrace support" ON)

  define_option(ARROW_WITH_OPENTELEMETRY
                "Build libraries with OpenTelemetry support for distributed tracing" OFF)

  define_option(ARROW_WITH_BROTLI "Build with Brotli compression" OFF)
  define_option(ARROW_WITH_BZ2 "Build with BZ2 compression" OFF)
  define_option(ARROW_WITH_LZ4 "Build with lz4 compression" OFF)
  define_option(ARROW_WITH_SNAPPY "Build with Snappy compression" OFF)
  define_option(ARROW_WITH_ZLIB "Build with zlib compression" OFF)
  define_option(ARROW_WITH_ZSTD "Build with zstd compression" OFF)

  define_option(ARROW_WITH_UCX
                "Build with UCX transport for Arrow Flight;(only used if ARROW_FLIGHT is ON)"
                OFF)

  define_option(ARROW_WITH_UTF8PROC
                "Build with support for Unicode properties using the utf8proc library;(only used if ARROW_COMPUTE is ON or ARROW_GANDIVA is ON)"
                ON)
  define_option(ARROW_WITH_RE2
                "Build with support for regular expressions using the re2 library;(only used if ARROW_COMPUTE or ARROW_GANDIVA is ON)"
                ON)

  #----------------------------------------------------------------------
  if(MSVC_TOOLCHAIN)
    set_option_category("MSVC")

    define_option(MSVC_LINK_VERBOSE
                  "Pass verbose linking options when linking libraries and executables"
                  OFF)

    define_option_string(BROTLI_MSVC_STATIC_LIB_SUFFIX
                         "Brotli static lib suffix used on Windows with MSVC" "-static")

    define_option_string(PROTOBUF_MSVC_STATIC_LIB_SUFFIX
                         "Protobuf static lib suffix used on Windows with MSVC" "")

    define_option_string(RE2_MSVC_STATIC_LIB_SUFFIX
                         "re2 static lib suffix used on Windows with MSVC" "_static")

    if(DEFINED ENV{CONDA_PREFIX})
      # Conda package changes the output name.
      # https://github.com/conda-forge/snappy-feedstock/blob/master/recipe/windows-static-lib-name.patch
      set(SNAPPY_MSVC_STATIC_LIB_SUFFIX_DEFAULT "_static")
    else()
      set(SNAPPY_MSVC_STATIC_LIB_SUFFIX_DEFAULT "")
    endif()
    define_option_string(SNAPPY_MSVC_STATIC_LIB_SUFFIX
                         "Snappy static lib suffix used on Windows with MSVC"
                         "${SNAPPY_MSVC_STATIC_LIB_SUFFIX_DEFAULT}")

    define_option_string(LZ4_MSVC_STATIC_LIB_SUFFIX
                         "Lz4 static lib suffix used on Windows with MSVC" "_static")

    define_option_string(ZSTD_MSVC_STATIC_LIB_SUFFIX
                         "ZStd static lib suffix used on Windows with MSVC" "_static")

    define_option(ARROW_USE_STATIC_CRT "Build Arrow with statically linked CRT" OFF)
  endif()

  #----------------------------------------------------------------------
  set_option_category("Parquet")

  define_option(PARQUET_MINIMAL_DEPENDENCY
                "Depend only on Thirdparty headers to build libparquet.;\
Always OFF if building binaries" OFF)

  define_option(PARQUET_BUILD_EXECUTABLES
                "Build the Parquet executable CLI tools. Requires static libraries to be built."
                OFF)

  define_option(PARQUET_BUILD_EXAMPLES
                "Build the Parquet examples. Requires static libraries to be built." OFF)

  define_option(PARQUET_REQUIRE_ENCRYPTION
                "Build support for encryption. Fail if OpenSSL is not found" OFF)

  #----------------------------------------------------------------------
  set_option_category("Gandiva")

  # ARROW-3860: Temporary workaround
  define_option(ARROW_GANDIVA_STATIC_LIBSTDCPP
                "Include -static-libstdc++ -static-libgcc when linking with;Gandiva static libraries"
                OFF)

  define_option_string(ARROW_GANDIVA_PC_CXX_FLAGS
                       "Compiler flags to append when pre-compiling Gandiva operations"
                       "")

  #----------------------------------------------------------------------
  set_option_category("Advanced developer")

  define_option(ARROW_EXTRA_ERROR_CONTEXT
                "Compile with extra error context (line numbers, code)" OFF)

  define_option(ARROW_OPTIONAL_INSTALL
                "If enabled install ONLY targets that have already been built. Please be;\
advised that if this is enabled 'install' will fail silently on components;\
that have not been built"
                OFF)

  option(ARROW_BUILD_CONFIG_SUMMARY_JSON "Summarize build configuration in a JSON file"
         ON)

  resolve_option_dependencies()
endif()

macro(validate_config)
  foreach(category ${ARROW_OPTION_CATEGORIES})
    set(option_names ${ARROW_${category}_OPTION_NAMES})

    foreach(name ${option_names})
      set(possible_values ${${name}_OPTION_POSSIBLE_VALUES})
      set(value "${${name}}")
      if(possible_values)
        if(NOT "${value}" IN_LIST possible_values)
          message(FATAL_ERROR "Configuration option ${name} got invalid value '${value}'. "
                              "Allowed values: ${${name}_OPTION_ENUM}.")
        endif()
      endif()
    endforeach()

  endforeach()
endmacro()

macro(config_summary_message)
  message(STATUS "---------------------------------------------------------------------")
  message(STATUS "Arrow version:                                 ${ARROW_VERSION}")
  message(STATUS)
  message(STATUS "Build configuration summary:")

  message(STATUS "  Generator: ${CMAKE_GENERATOR}")
  message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
  message(STATUS "  Source directory: ${CMAKE_CURRENT_SOURCE_DIR}")
  message(STATUS "  Install prefix: ${CMAKE_INSTALL_PREFIX}")
  if(${CMAKE_EXPORT_COMPILE_COMMANDS})
    message(STATUS "  Compile commands: ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json"
    )
  endif()

  foreach(category ${ARROW_OPTION_CATEGORIES})

    message(STATUS)
    message(STATUS "${category} options:")
    message(STATUS)

    set(option_names ${ARROW_${category}_OPTION_NAMES})

    foreach(name ${option_names})
      set(value "${${name}}")
      if("${value}" STREQUAL "")
        set(value "\"\"")
      endif()

      set(description ${${name}_OPTION_DESCRIPTION})

      if(NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
        set(summary "=${value} [default=${${name}_OPTION_ENUM}]")
      else()
        set(summary "=${value} [default=${${name}_OPTION_DEFAULT}]")
      endif()

      message(STATUS "  ${name}${summary}")
      foreach(description_line ${description})
        message(STATUS "      ${description_line}")
      endforeach()
    endforeach()

  endforeach()

  if(ARROW_PYTHON)
    message(WARNING "ARROW_PYTHON is deprecated. Use CMake presets instead.")
  endif()
endmacro()

macro(config_summary_json)
  set(summary "${CMAKE_CURRENT_BINARY_DIR}/cmake_summary.json")
  message(STATUS "  Outputting build configuration summary to ${summary}")
  file(WRITE ${summary} "{\n")

  foreach(category ${ARROW_OPTION_CATEGORIES})
    foreach(name ${ARROW_${category}_OPTION_NAMES})
      file(APPEND ${summary} "\"${name}\": \"${${name}}\",\n")
    endforeach()
  endforeach()

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
endmacro()

macro(config_summary_cmake_setters path)
  file(WRITE ${path} "# Options used to build arrow:")

  foreach(category ${ARROW_OPTION_CATEGORIES})
    file(APPEND ${path} "\n\n## ${category} options:")
    foreach(name ${ARROW_${category}_OPTION_NAMES})
      set(description ${${name}_OPTION_DESCRIPTION})
      foreach(description_line ${description})
        file(APPEND ${path} "\n### ${description_line}")
      endforeach()
      file(APPEND ${path} "\nset(${name} \"${${name}}\")")
    endforeach()
  endforeach()

endmacro()

#----------------------------------------------------------------------
# Compute default values for omitted variables

if(NOT ARROW_GIT_ID)
  execute_process(COMMAND "git" "log" "-n1" "--format=%H"
                  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                  OUTPUT_VARIABLE ARROW_GIT_ID
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
if(NOT ARROW_GIT_DESCRIPTION)
  execute_process(COMMAND "git" "describe" "--tags" "--dirty"
                  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                  ERROR_QUIET
                  OUTPUT_VARIABLE ARROW_GIT_DESCRIPTION
                  OUTPUT_STRIP_TRAILING_WHITESPACE)
endif()
