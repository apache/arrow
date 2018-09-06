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

#######################################
# arrow_gpu
#######################################

if (DEFINED ENV{CUDA_HOME})
  set(CUDA_TOOLKIT_ROOT_DIR "$ENV{CUDA_HOME}")
endif()

find_package(CUDA REQUIRED)
include_directories(SYSTEM ${CUDA_INCLUDE_DIRS})

message(STATUS "CUDA Libraries: ${CUDA_LIBRARIES}")

set(ARROW_GPU_SRCS
  cuda_arrow_ipc.cc
  cuda_context.cc
  cuda_memory.cc
)

set(ARROW_GPU_SHARED_LINK_LIBS
  ${CUDA_LIBRARIES}
  ${CUDA_CUDA_LIBRARY}
)

ADD_ARROW_LIB(arrow_gpu
  SOURCES ${ARROW_GPU_SRCS}
  OUTPUTS ARROW_GPU_LIBRARIES
  DEPENDENCIES metadata_fbs
  SHARED_LINK_FLAGS ""
  SHARED_LINK_LIBS arrow_shared ${ARROW_GPU_SHARED_LINK_LIBS}
  # Static arrow_gpu must also link against CUDA shared libs
  STATIC_LINK_LIBS ${ARROW_GPU_SHARED_LINK_LIBS}
)

foreach(LIB_TARGET ${ARROW_GPU_LIBRARIES})
  target_compile_definitions(${LIB_TARGET}
    PRIVATE ARROW_EXPORTING)
endforeach()

# CUDA build version
configure_file(cuda_version.h.in
  "${CMAKE_CURRENT_BINARY_DIR}/cuda_version.h"
  @ONLY)

install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/cuda_version.h"
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/gpu")

install(FILES
  cuda_api.h
  cuda_arrow_ipc.h
  cuda_context.h
  cuda_memory.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/gpu")

# pkg-config support
configure_file(arrow-gpu.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow-gpu.pc"
  @ONLY)

install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow-gpu.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

set(ARROW_GPU_TEST_LINK_LIBS
  # ARROW-2561: Must link statically against arrow and arrow_gpu to avoid double free
  arrow_gpu_static
  ${ARROW_TEST_LINK_LIBS})

if (ARROW_BUILD_TESTS)
  ADD_ARROW_TEST(cuda-test
    STATIC_LINK_LIBS ${ARROW_GPU_TEST_LINK_LIBS}
    NO_VALGRIND)
endif()

if (ARROW_BUILD_BENCHMARKS)
  cuda_add_executable(cuda-benchmark cuda-benchmark.cc)
  target_link_libraries(cuda-benchmark
    arrow_gpu_shared
    gtest
    ${ARROW_BENCHMARK_LINK_LIBS})
endif()
