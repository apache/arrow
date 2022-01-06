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

# Add ${CMAKE_INSTALL_PREFIX}/arrow_matlab to the MATLAB Search Path so that MATLAB can find
# the installed libraries.

# Convert Matlab_MAIN_PROGRAM, INSTALL_DIR and TOOLS_DIR to use OS native path notation.
file(TO_NATIVE_PATH ${Matlab_MAIN_PROGRAM} NATIVE_MATLAB_MAIN_PROGRAM)
file(TO_NATIVE_PATH ${INSTALL_DIR} NATIVE_INSTALL_DIR)
file(TO_NATIVE_PATH ${TOOLS_DIR} NATIVE_TOOLS_DIR)

# Initialize an instance of MATLAB and call the MATLAB function, addInstallDirToSearchPath,
# defined in ${NATIVE_TOOLS_DIR}.
# Flags to pass to MATLAB:
#     -sd:    startup directory for the MATLAB
#     -batch: non-interactive script execution
execute_process(COMMAND "${NATIVE_MATLAB_MAIN_PROGRAM}" -sd "${NATIVE_TOOLS_DIR}" -batch
                        "addInstallDirToSearchPath('${INSTALL_DIR}')"
                RESULT_VARIABLE MATLAB_EXIT_CODE)

if(MATLAB_EXIT_CODE EQUAL "1")
  # Get path to MATLAB pathdef.m file. This is the location of the default MATLAB Search Path
  set(MATLAB_PATHDEF_FILE "${Matlab_MAIN_PROGRAM}/toolbox/local/pathdef.m")
  file(TO_NATIVE_PATH ${MATLAB_PATHDEF_FILE} NATIVE_MATLAB_PATHDEF_FILE)

  message(FATAL_ERROR "Failed to add the installation directory, ${NATIVE_INSTALL_DIR}, to the MATLAB Search Path. This may be due to the current user lacking the necessary filesystem permissions to modify ${NATIVE_MATLAB_PATHDEF_FILE}. In order to complete the installation process, ${NATIVE_INSTALL_DIR} must be added to the MATLAB Search Path using the \"addpath\" and \"savepath\" MATLAB commands or by resolving the permissions issues and re-running the CMake install target."
  )
endif()
