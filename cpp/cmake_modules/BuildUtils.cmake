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

function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LIB STATIC_LIB)
  set(multi_value_args DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_STATIC_LIB AND ARG_SHARED_LIB)
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()

    SET(AUG_LIB_NAME "${LIB_NAME}_static")
    add_library(${AUG_LIB_NAME} STATIC IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    if(ARG_DEPS)
      set_target_properties(${AUG_LIB_NAME}
        PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
    endif()
    message("Added static library dependency ${AUG_LIB_NAME}: ${ARG_STATIC_LIB}")

    SET(AUG_LIB_NAME "${LIB_NAME}_shared")
    add_library(${AUG_LIB_NAME} SHARED IMPORTED)

    if(WIN32)
        # Mark the ”.lib” location as part of a Windows DLL
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_IMPLIB "${ARG_SHARED_LIB}")
    else()
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    endif()
    if(ARG_DEPS)
      set_target_properties(${AUG_LIB_NAME}
        PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
    endif()
    message("Added shared library dependency ${AUG_LIB_NAME}: ${ARG_SHARED_LIB}")
  elseif(ARG_STATIC_LIB)
    SET(AUG_LIB_NAME "${LIB_NAME}_static")
    add_library(${AUG_LIB_NAME} STATIC IMPORTED)
    set_target_properties(${AUG_LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    if(ARG_DEPS)
      set_target_properties(${AUG_LIB_NAME}
        PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
    endif()
    message("Added static library dependency ${AUG_LIB_NAME}: ${ARG_STATIC_LIB}")
  elseif(ARG_SHARED_LIB)
    SET(AUG_LIB_NAME "${LIB_NAME}_shared")
    add_library(${AUG_LIB_NAME} SHARED IMPORTED)

    if(WIN32)
        # Mark the ”.lib” location as part of a Windows DLL
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_IMPLIB "${ARG_SHARED_LIB}")
    else()
        set_target_properties(${AUG_LIB_NAME}
            PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    endif()
    message("Added shared library dependency ${AUG_LIB_NAME}: ${ARG_SHARED_LIB}")
    if(ARG_DEPS)
      set_target_properties(${AUG_LIB_NAME}
        PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
    endif()
  else()
    message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
  endif()
endfunction()

# \arg OUTPUTS list to append built targets to
function(ADD_ARROW_LIB LIB_NAME)
  set(options BUILD_SHARED BUILD_STATIC)
  set(one_value_args SHARED_LINK_FLAGS)
  set(multi_value_args SOURCES OUTPUTS
                       STATIC_LINK_LIBS
                       SHARED_LINK_LIBS
                       SHARED_PRIVATE_LINK_LIBS
                       EXTRA_INCLUDES
                       PRIVATE_INCLUDES
                       DEPENDENCIES)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if (ARG_OUTPUTS)
    set(${ARG_OUTPUTS})
  endif()

  # Allow overriding ARROW_BUILD_SHARED and ARROW_BUILD_STATIC
  if (ARG_BUILD_SHARED)
    set(BUILD_SHARED ${ARG_BUILD_SHARED})
  else ()
    set(BUILD_SHARED ${ARROW_BUILD_SHARED})
  endif()
  if (ARG_BUILD_STATIC)
    set(BUILD_STATIC ${ARG_BUILD_STATIC})
  else ()
    set(BUILD_STATIC ${ARROW_BUILD_STATIC})
  endif()

  if(MSVC)
    # MSVC needs to compile C++ separately for each library kind (shared and static)
    # because of dllexport declarations
    set(LIB_DEPS ${ARG_SOURCES})
    set(EXTRA_DEPS ${ARG_DEPENDENCIES})

    if (ARG_EXTRA_INCLUDES)
      set(LIB_INCLUDES ${ARG_EXTRA_INCLUDES})
    endif()
  else()
    # Otherwise, generate a single "objlib" from all C++ modules and link
    # that "objlib" into each library kind, to avoid compiling twice
    add_library(${LIB_NAME}_objlib OBJECT
      ${ARG_SOURCES})
    # Necessary to make static linking into other shared libraries work properly
    set_property(TARGET ${LIB_NAME}_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
    if (ARG_DEPENDENCIES)
      add_dependencies(${LIB_NAME}_objlib ${ARG_DEPENDENCIES})
    endif()
    set(LIB_DEPS $<TARGET_OBJECTS:${LIB_NAME}_objlib>)
    set(LIB_INCLUDES)
    set(EXTRA_DEPS)

    if (ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_objlib)
    endif()

    if (ARG_EXTRA_INCLUDES)
      target_include_directories(${LIB_NAME}_objlib SYSTEM PUBLIC
        ${ARG_EXTRA_INCLUDES}
        )
    endif()
    if (ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_objlib PRIVATE
        ${ARG_PRIVATE_INCLUDES})
    endif()
  endif()

  set(RUNTIME_INSTALL_DIR bin)

  if (BUILD_SHARED)
    add_library(${LIB_NAME}_shared SHARED ${LIB_DEPS})
    if (EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_shared ${EXTRA_DEPS})
    endif()

    if (ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_shared)
    endif()

    if (LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_shared SYSTEM PUBLIC
        ${ARG_EXTRA_INCLUDES}
        )
    endif()

    if (ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_shared PRIVATE
        ${ARG_PRIVATE_INCLUDES})
    endif()

    if(APPLE)
      # On OS X, you can avoid linking at library load time and instead
      # expecting that the symbols have been loaded separately. This happens
      # with libpython* where there can be conflicts between system Python and
      # the Python from a thirdparty distribution
      set(ARG_SHARED_LINK_FLAGS
        "-undefined dynamic_lookup ${ARG_SHARED_LINK_FLAGS}")
    endif()

    set_target_properties(${LIB_NAME}_shared
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      RUNTIME_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      PDB_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      LINK_FLAGS "${ARG_SHARED_LINK_FLAGS}"
      OUTPUT_NAME ${LIB_NAME}
      VERSION "${ARROW_FULL_SO_VERSION}"
      SOVERSION "${ARROW_SO_VERSION}")

    target_link_libraries(${LIB_NAME}_shared
      LINK_PUBLIC ${ARG_SHARED_LINK_LIBS}
      LINK_PRIVATE ${ARG_SHARED_PRIVATE_LINK_LIBS})

    if (ARROW_RPATH_ORIGIN)
        if (APPLE)
            set(_lib_install_rpath "@loader_path")
        else()
            set(_lib_install_rpath "\$ORIGIN")
        endif()
        set_target_properties(${LIB_NAME}_shared PROPERTIES
            INSTALL_RPATH ${_lib_install_rpath})
    endif()

    if (APPLE)
      if (ARROW_INSTALL_NAME_RPATH)
        set(_lib_install_name "@rpath")
      else()
        set(_lib_install_name "${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
      endif()
      set_target_properties(${LIB_NAME}_shared
        PROPERTIES
        BUILD_WITH_INSTALL_RPATH ON
        INSTALL_NAME_DIR "${_lib_install_name}")
    endif()

    install(TARGETS ${LIB_NAME}_shared
      EXPORT ${PROJECT_NAME}-targets
      RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()

  if (BUILD_STATIC)
    add_library(${LIB_NAME}_static STATIC ${LIB_DEPS})
    if(EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_static ${EXTRA_DEPS})
    endif()

    if (ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_static)
    endif()

    if (LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_static SYSTEM PUBLIC
        ${ARG_EXTRA_INCLUDES}
        )
    endif()

    if (ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_static PRIVATE
        ${ARG_PRIVATE_INCLUDES})
    endif()

    if (MSVC)
      set(LIB_NAME_STATIC ${LIB_NAME}_static)
    else()
      set(LIB_NAME_STATIC ${LIB_NAME})
    endif()

    set_target_properties(${LIB_NAME}_static
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      OUTPUT_NAME ${LIB_NAME_STATIC})

    target_link_libraries(${LIB_NAME}_static
      LINK_PUBLIC ${ARG_STATIC_LINK_LIBS})

    install(TARGETS ${LIB_NAME}_static
      EXPORT ${PROJECT_NAME}-targets
      RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()

  # Modify variable in calling scope
  if (ARG_OUTPUTS)
    set(${ARG_OUTPUTS} ${${ARG_OUTPUTS}} PARENT_SCOPE)
  endif()
endfunction()


############################################################
# Benchmarking
############################################################
# Add a new micro benchmark, with or without an executable that should be built.
# If benchmarks are enabled then they will be run along side unit tests with ctest.
# 'make runbenchmark' and 'make unittest' to build/run only benchmark or unittests,
# respectively.
#
# REL_BENCHMARK_NAME is the name of the benchmark app. It may be a single component
# (e.g. monotime-benchmark) or contain additional components (e.g.
# net/net_util-benchmark). Either way, the last component must be a globally
# unique name.

# The benchmark will registered as unit test with ctest with a label
# of 'benchmark'.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_ARROW_BENCHMARK REL_BENCHMARK_NAME)
  if(NO_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCHMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME}.cc)
    # This benchmark has a corresponding .cc file, set it up as an executable.
    set(BENCHMARK_PATH "${EXECUTABLE_OUTPUT_PATH}/${BENCHMARK_NAME}")
    add_executable(${BENCHMARK_NAME} "${REL_BENCHMARK_NAME}.cc")
    target_link_libraries(${BENCHMARK_NAME} ${ARROW_BENCHMARK_LINK_LIBS})
    add_dependencies(runbenchmark ${BENCHMARK_NAME})
    set(NO_COLOR "--color_print=false")
  else()
    # No executable, just invoke the benchmark (probably a script) directly.
    set(BENCHMARK_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME})
    set(NO_COLOR "")
  endif()

  add_test(${BENCHMARK_NAME}
    ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} benchmark ${BENCHMARK_PATH} ${NO_COLOR})
  set_tests_properties(${BENCHMARK_NAME} PROPERTIES LABELS "benchmark")
  if(ARGN)
    set_tests_properties(${BENCHMARK_NAME} PROPERTIES ${ARGN})
  endif()
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_BENCHMARKS.
function(ADD_ARROW_BENCHMARK_DEPENDENCIES REL_BENCHMARK_NAME)
  if(NO_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  add_dependencies(${BENCHMARK_NAME} ${ARGN})
endfunction()

# A wrapper for target_link_libraries() that is compatible with NO_BENCHMARKS.
function(ARROW_BENCHMARK_LINK_LIBRARIES REL_BENCHMARK_NAME)
    if(NO_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCHMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  target_link_libraries(${BENCHMARK_NAME} ${ARGN})
endfunction()


############################################################
# Testing
############################################################
# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# The unit test is added with a label of "unittest" to support filtering with
# ctest.
#
# Arguments after the test name will be passed to set_tests_properties().
#
# \arg PREFIX a string to append to the name of the test executable. For
# example, if you have src/arrow/foo/bar-test.cc, then PREFIX "foo" will create
# test executable foo-bar-test
# \arg LABELS the unit test label or labels to assign the unit tests
# to. By default, unit tests will go in the "unittest" group, but if we have
# multiple unit tests in some subgroup, you can assign a test to multiple
# groups using the syntax unittest;GROUP2;GROUP3. Custom targets for the group
# names must exist
function(ADD_ARROW_TEST REL_TEST_NAME)
  set(options NO_VALGRIND)
  set(one_value_args)
  set(multi_value_args STATIC_LINK_LIBS EXTRA_LINK_LIBS EXTRA_INCLUDES EXTRA_DEPENDENCIES
    LABELS PREFIX)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if (NOT "${ARROW_TEST_INCLUDE_LABELS}" STREQUAL "")
    set(_SKIP_TEST TRUE)
    foreach (_INCLUDED_LABEL ${ARG_LABELS})
      if ("${ARG_LABELS}" MATCHES "${_INCLUDED_LABEL}")
        set(_SKIP_TEST FALSE)
      endif()
    endforeach()
    if (_SKIP_TEST)
      return()
    endif()
  endif()

  if (NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(ARG_PREFIX)
    set(TEST_NAME "${ARG_PREFIX}-${TEST_NAME}")
  endif()

  if (ARG_LABELS)
    set(ARG_LABELS "${ARG_LABELS}")
  else()
    set(ARG_LABELS unittest)
  endif()

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")

    if (ARG_STATIC_LINK_LIBS)
      # Customize link libraries
      target_link_libraries(${TEST_NAME} ${ARG_STATIC_LINK_LIBS})
    else()
      target_link_libraries(${TEST_NAME} ${ARROW_TEST_LINK_LIBS})
    endif()

    if (ARG_EXTRA_LINK_LIBS)
      target_link_libraries(${TEST_NAME} ${ARG_EXTRA_LINK_LIBS})
    endif()

    if (ARG_EXTRA_INCLUDES)
      target_include_directories(${TEST_NAME} SYSTEM PUBLIC
        ${ARG_EXTRA_INCLUDES}
        )
    endif()

    if (ARG_EXTRA_DEPENDENCIES)
      add_dependencies(${TEST_NAME} ${ARG_EXTRA_DEPENDENCIES})
    endif()

    foreach (TEST_LABEL ${ARG_LABELS})
      add_dependencies(${TEST_LABEL} ${TEST_NAME})
    endforeach()
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  if (ARROW_TEST_MEMCHECK AND NOT ARG_NO_VALGRIND)
    SET_PROPERTY(TARGET ${TEST_NAME}
      APPEND_STRING PROPERTY
      COMPILE_FLAGS " -DARROW_VALGRIND")
    add_test(${TEST_NAME}
      bash -c "cd '${CMAKE_SOURCE_DIR}'; \
               valgrind --suppressions=valgrind.supp --tool=memcheck --gen-suppressions=all \
                 --leak-check=full --leak-check-heuristics=stdstring --error-exitcode=1 ${TEST_PATH}")
  elseif(MSVC)
    add_test(${TEST_NAME} ${TEST_PATH})
  else()
    add_test(${TEST_NAME}
      ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_PATH})
  endif()

  set_property(TEST ${TEST_NAME}
    APPEND PROPERTY
    LABELS ${ARG_LABELS})
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_TESTS.
function(ADD_ARROW_TEST_DEPENDENCIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_dependencies(${TEST_NAME} ${ARGN})
endfunction()

# A wrapper for target_link_libraries() that is compatible with NO_TESTS.
function(ARROW_TEST_LINK_LIBRARIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  target_link_libraries(${TEST_NAME} ${ARGN})
endfunction()

############################################################
# Fuzzing
############################################################
# Add new fuzzing test executable.
#
# The single source file must define a function:
#   extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size)
#
# No main function must be present within the source file!
#
function(ADD_ARROW_FUZZING REL_FUZZING_NAME)
  if(NO_FUZZING)
    return()
  endif()

  if (ARROW_BUILD_STATIC)
    set(FUZZ_LINK_LIBS arrow_static)
  else()
    set(FUZZ_LINK_LIBS arrow_shared)
  endif()

  add_executable(${REL_FUZZING_NAME} "${REL_FUZZING_NAME}.cc")
  target_link_libraries(${REL_FUZZING_NAME} ${FUZZ_LINK_LIBS})
  target_compile_options(${REL_FUZZING_NAME}
      PRIVATE "-fsanitize=fuzzer")
  set_target_properties(${REL_FUZZING_NAME}
      PROPERTIES
      LINK_FLAGS "-fsanitize=fuzzer")
endfunction()
