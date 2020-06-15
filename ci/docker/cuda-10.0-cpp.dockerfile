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

# FROM nvidia/cuda:10.0-devel-ubuntu18.04


# Prioritize system packages and local installation
# The following dependencies will be downloaded due to missing/invalid packages
# provided by the distribution:
# - libc-ares-dev does not install CMake config files
# - flatbuffer is not packaged
# - libgtest-dev only provide sources
# - libprotobuf-dev only provide sources
# - thrift is too old
# ENV ARROW_BUILD_STATIC=OFF \
#     ARROW_BUILD_TESTS=ON \
#     ARROW_COMPUTE=OFF \
#     ARROW_CSV=OFF \
#     ARROW_CUDA=ON \
#     ARROW_DATASET=OFF \
#     ARROW_DEPENDENCY_SOURCE=SYSTEM \
#     ARROW_FILESYSTEM=OFF \
#     ARROW_FLIGHT=OFF \
#     ARROW_HOME=/usr/local \
#     ARROW_INSTALL_NAME_RPATH=OFF \
#     ARROW_NO_DEPRECATED_API=ON \
#     ARROW_PLASMA=ON \
#     ARROW_USE_CCACHE=ON \
#     CUDA_LIB_PATH=${LIBRARY_PATH} \
#     GTest_SOURCE=BUNDLED \
#     ORC_SOURCE=BUNDLED \
#     PATH=/usr/lib/ccache/:$PATH \
#     Thrift_SOURCE=BUNDLED

export CMAKE_ARGS="-DCUDA_CUDA_LIBRARY=/usr/local/cuda/lib64/stubs/libcuda.so"
