# Copyright 2012 Cloudera Inc.
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

# Helper function to generate build rules.  For each input thrift file, this function will
# generate a rule that maps the input file to the output c++ file.
# Thrift will generate multiple output files for each input (including java files) and
# ideally, we'd specify all of the outputs for dependency tracking.
# Unfortunately, it's not easy to figure out all the output files without parsing the
# thrift input. (TODO: can thrift tells us what the java output files will be?)
# The list of output files is used for build dependency tracking so it's not necessary to
# capture all the output files.
#
# To call this function, pass it the output file list followed by the input thrift files:
#    i.e. HS2_THRIFT_GEN(OUTPUT_FILES, ${THRIFT_FILES})
#
# cmake seems to be case sensitive for some keywords. Changing the first IF check to lower
# case makes it not work.  TODO: investigate this
function(HS2_THRIFT_GEN VAR)
  IF (NOT ARGN)
    MESSAGE(SEND_ERROR "Error: THRIFT_GEN called without any src files")
    RETURN()
  ENDIF(NOT ARGN)

  set(${VAR})
  foreach(FIL ${ARGN})
    # Get full path
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    # Get basename
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    set(GEN_DIR "${OUTPUT_DIR}/arrow/dbi/hiveserver2")

    # All the output files we can determine based on filename.
    #   - Does not include .skeleton.cpp files
    #   - Does not include java output files
    set(OUTPUT_BE_FILE
      "${GEN_DIR}/${FIL_WE}_types.cpp"
      "${GEN_DIR}/${FIL_WE}_types.h"
      "${GEN_DIR}/${FIL_WE}_constants.cpp"
      "${GEN_DIR}/${FIL_WE}_constants.h")
    list(APPEND ${VAR} ${OUTPUT_BE_FILE})

    # BeeswaxService thrift generation
    # It depends on hive_meta_store, which in turn depends on fb303.
    # The java dependency is handled by maven.
    # We need to generate C++ src file for the parent dependencies using the "-r" option.
    set(CPP_ARGS -nowarn --gen cpp -out ${GEN_DIR})
    IF (FIL STREQUAL "beeswax.thrift")
      set(CPP_ARGS -r -nowarn --gen cpp -out ${GEN_DIR})
    ENDIF(FIL STREQUAL "beeswax.thrift")

    # Be able to include generated ErrorCodes.thrift file
    set(CPP_ARGS ${CPP_ARGS} -I ${CMAKE_CURRENT_BINARY_DIR})

    add_custom_command(
      OUTPUT ${OUTPUT_BE_FILE}
      COMMAND ${THRIFT_COMPILER} ${CPP_ARGS} ${FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running thrift compiler on ${FIL}"
	  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      VERBATIM
    )
  endforeach(FIL)

  set(${VAR} ${${VAR}} PARENT_SCOPE)
endfunction(HS2_THRIFT_GEN)

message("Using Thrift compiler: ${THRIFT_COMPILER}")

set(OUTPUT_DIR ${CMAKE_BINARY_DIR}/src)
file(MAKE_DIRECTORY ${OUTPUT_DIR})

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ErrorCodes.thrift
  COMMAND python generate_error_codes.py ${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS generate_error_codes.py
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

set(SRC_FILES
  ${CMAKE_CURRENT_BINARY_DIR}/ErrorCodes.thrift
  beeswax.thrift
  TCLIService.thrift
  ExecStats.thrift
  ImpalaService.thrift
  Status.thrift
  Types.thrift
)

SET_SOURCE_FILES_PROPERTIES(Status.thrift PROPERTIES OBJECT_DEPENDS
  ${CMAKE_CURRENT_BINARY_DIR}/ErrorCodes.thrift)

# Create a build command for each of the thrift src files and generate
# a list of files they produce
HS2_THRIFT_GEN(THRIFT_ALL_FILES ${SRC_FILES})

# Add a custom target that generates all the thrift files
add_custom_target(hs2-thrift-cpp ALL DEPENDS ${THRIFT_ALL_FILES})

add_custom_target(hs2-thrift-generated-files-error DEPENDS
  ${CMAKE_CURRENT_BINARY_DIR}/ErrorCodes.thrift)
add_dependencies(hs2-thrift-cpp hs2-thrift-generated-files-error)
