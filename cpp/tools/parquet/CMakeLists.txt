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

if(PARQUET_BUILD_EXECUTABLES)
  set(PARQUET_TOOLS parquet-dump-schema parquet-reader parquet-scan)

  foreach(TOOL ${PARQUET_TOOLS})
    add_executable(${TOOL} "${TOOL}.cc")
    target_link_libraries(${TOOL} parquet_static)
    # Avoid unsetting RPATH when installing
    set_target_properties(${TOOL} PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
    install(TARGETS ${TOOL} ${INSTALL_IS_OPTIONAL}
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  endforeach(TOOL)

  add_dependencies(parquet ${PARQUET_TOOLS})
endif()
