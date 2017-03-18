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

function(ADD_ARROW_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LINK_FLAGS)
  set(multi_value_args SOURCES STATIC_LINK_LIBS STATIC_PRIVATE_LINK_LIBS SHARED_LINK_LIBS SHARED_PRIVATE_LINK_LIBS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  add_library(${LIB_NAME}_objlib OBJECT
      ${ARG_SOURCES}
  )
  if (ARG_STATIC_LINK_LIBS)
    add_dependencies(${LIB_NAME}_objlib ${ARG_STATIC_LINK_LIBS})
  endif()
  if (ARG_STATIC_PRIVATE_LINK_LIBS)
    add_dependencies(${LIB_NAME}_objlib ${ARG_STATIC_PRIVATE_LINK_LIBS})
  endif()
  if (ARG_SHARED_LINK_LIBS)
    add_dependencies(${LIB_NAME}_objlib ${ARG_SHARED_LINK_LIBS})
  endif()
  if(ARG_SHARED_PRIVATE_LINK_LIBS)
    add_dependencies(${LIB_NAME}_objlib ${ARG_SHARED_PRIVATE_LINK_LIBS})
  endif()

  # Necessary to make static linking into other shared libraries work properly
  set_property(TARGET ${LIB_NAME}_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)

  if (ARROW_BUILD_SHARED)
    add_library(${LIB_NAME}_shared SHARED $<TARGET_OBJECTS:${LIB_NAME}_objlib>)
    if(APPLE)
      set_target_properties(${LIB_NAME}_shared PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
    endif()
    set_target_properties(${LIB_NAME}_shared
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      LINK_FLAGS "${ARG_SHARED_LINK_FLAGS}"
      OUTPUT_NAME ${LIB_NAME})
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
  
    install(TARGETS ${LIB_NAME}_shared
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()
  
  if (ARROW_BUILD_STATIC)
      add_library(${LIB_NAME}_static STATIC $<TARGET_OBJECTS:${LIB_NAME}_objlib>)
    set_target_properties(${LIB_NAME}_static
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      OUTPUT_NAME ${LIB_NAME})
  
  target_link_libraries(${LIB_NAME}_static
      LINK_PUBLIC ${ARG_STATIC_LINK_LIBS}
      LINK_PRIVATE ${ARG_STATIC_PRIVATE_LINK_LIBS})
  
  install(TARGETS ${LIB_NAME}_static
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
  endif()
  
  if (APPLE)
      set_target_properties(${LIB_NAME}_shared
      PROPERTIES
      BUILD_WITH_INSTALL_RPATH ON
      INSTALL_NAME_DIR "@rpath")
  endif()

endfunction()

