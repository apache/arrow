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

find_package(PythonInterp REQUIRED)

if (PYTHON_EXECUTABLE)
    if(DEFINED ENV{TENSORFLOW_INCLUDE_DIR})
        set(TensorFlow_INCLUDE_DIRS $ENV{TENSORFLOW_INCLUDE_DIR})
        set(TensorFlow_LIBRARY_DIRS "")
    else()
        execute_process(COMMAND "${PYTHON_EXECUTABLE}" -c
            "import tensorflow; print(tensorflow.sysconfig.get_include())"
            OUTPUT_VARIABLE TensorFlow_INCLUDE_DIRS
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        execute_process(COMMAND "${PYTHON_EXECUTABLE}" -c
            "import tensorflow; print(' '.join(tensorflow.sysconfig.get_link_flags()).strip())"
            OUTPUT_VARIABLE TensorFlow_LIBRARY_DIRS
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    endif()
else()
    message(STATUS "Python executable not found.")
    set(TensorFlow_FOUND FALSE)
    return()
endif()

if (TensorFlow_INCLUDE_DIRS)
    set(TensorFlow_FOUND TRUE)
endif()
