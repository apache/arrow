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

#
# arrow_cuda
#

add_custom_target(arrow_cuda-all)
add_custom_target(arrow_cuda)
add_custom_target(arrow_cuda-benchmarks)
add_custom_target(arrow_cuda-tests)
add_dependencies(arrow_cuda-all arrow_cuda arrow_cuda-tests arrow_cuda-benchmarks)

if(DEFINED ENV{CUDA_HOME})
  set(CUDA_TOOLKIT_ROOT_DIR "$ENV{CUDA_HOME}")
endif()

set(ARROW_CUDA_LINK_LIBS arrow::flatbuffers)
if(CMAKE_VERSION VERSION_LESS 3.17)
  find_package(CUDA REQUIRED)
  set(ARROW_CUDA_SHARED_LINK_LIBS ${CUDA_CUDA_LIBRARY})
  include_directories(SYSTEM ${CUDA_INCLUDE_DIRS})
else()
  # find_package(CUDA) is deprecated, and for newer CUDA, it doesn't
  # recognize that the CUDA driver library is in the "stubs" dir, but
  # CUDAToolkit is only available in CMake >= 3.17
  find_package(CUDAToolkit REQUIRED)
  set(ARROW_CUDA_SHARED_LINK_LIBS CUDA::cuda_driver)
endif()

set(ARROW_CUDA_SRCS cuda_arrow_ipc.cc cuda_context.cc cuda_internal.cc cuda_memory.cc)

add_arrow_lib(arrow_cuda
              CMAKE_PACKAGE_NAME
              ArrowCUDA
              PKG_CONFIG_NAME
              arrow-cuda
              SOURCES
              ${ARROW_CUDA_SRCS}
              OUTPUTS
              ARROW_CUDA_LIBRARIES
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_shared
              ${ARROW_CUDA_LINK_LIBS}
              ${ARROW_CUDA_SHARED_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              Arrow::arrow_shared
              # Static arrow_cuda must also link against CUDA shared libs
              STATIC_LINK_LIBS
              ${ARROW_CUDA_LINK_LIBS}
              ${ARROW_CUDA_SHARED_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              Arrow::arrow_static
              ${ARROW_CUDA_SHARED_LINK_LIBS})

add_dependencies(arrow_cuda ${ARROW_CUDA_LIBRARIES})

foreach(LIB_TARGET ${ARROW_CUDA_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_EXPORTING)
endforeach()

# CUDA build version
configure_file(cuda_version.h.in "${CMAKE_CURRENT_BINARY_DIR}/cuda_version.h" @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/cuda_version.h"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/gpu")

arrow_install_all_headers("arrow/gpu")

if(ARROW_BUILD_SHARED)
  set(ARROW_CUDA_LIBRARY arrow_cuda_shared)
else()
  set(ARROW_CUDA_LIBRARY arrow_cuda_static)
endif()

set(ARROW_CUDA_TEST_LINK_LIBS ${ARROW_CUDA_LIBRARY} ${ARROW_TEST_LINK_LIBS})

if(ARROW_BUILD_TESTS)
  add_arrow_test(cuda_test STATIC_LINK_LIBS ${ARROW_CUDA_TEST_LINK_LIBS} NO_VALGRIND)
endif()

if(ARROW_BUILD_BENCHMARKS)
  add_arrow_benchmark(cuda_benchmark PREFIX "arrow-gpu")
  target_link_libraries(arrow-gpu-cuda-benchmark
                        PUBLIC ${ARROW_CUDA_LIBRARY} ${ARROW_GTEST_GTEST}
                               ${ARROW_BENCHMARK_LINK_LIBS})
  add_dependencies(arrow_cuda-benchmarks arrow-gpu-cuda-benchmark)
endif()
