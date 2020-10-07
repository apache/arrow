#!/bin/bash -ex
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

export GRPC_VERSION="1.32.0"
# The code in grpc to set macros for manylinux1 has been removed in 1.32.0.
# Disable platform detection and set the same macros as in older versions here.
# See: https://github.com/grpc/grpc/commit/23e1d30327923ac93beaeb6cdcfb0eef9be759c4#diff-dd49c38c34f9401e10f1921931d6c121
# Also prevent inclusion of src/core/lib/iomgr/port.h and manually define what
# that used to define for manylinux1, since the definitions for GPR_LINUX don't work.
export CFLAGS="-fPIC -DGPR_NO_AUTODETECT_PLATFORM \
-DGPR_PLATFORM_STRING='\"manylinux\"' \
-DGPR_POSIX_CRASH_HANDLER=1 \
-DGPR_CPU_POSIX=1 \
-DGPR_GCC_ATOMIC=1 \
-DGPR_GCC_TLS=1 \
-DGPR_LINUX=1 \
-DGPR_LINUX_LOG=1 \
-DGPR_SUPPORT_CHANNELS_FROM_FD=1 \
-DGPR_LINUX_ENV=1 \
-DGPR_POSIX_TMPFILE=1 \
-DGPR_POSIX_STRING=1 \
-DGPR_POSIX_SUBPROCESS=1 \
-DGPR_POSIX_SYNC=1 \
-DGPR_POSIX_TIME=1 \
-DGPR_HAS_PTHREAD_H=1 \
-DGPR_GETPID_IN_UNISTD_H=1 \
-DGPR_ARCH_64=1 \
-DGRPC_CORE_LIB_IOMGR_PORT_H \
-DGRPC_HAVE_ARPA_NAMESER=1 \
-DGRPC_HAVE_IFADDRS=1 \
-DGRPC_HAVE_IPV6_RECVPKTINFO=1 \
-DGRPC_HAVE_IP_PKTINFO=1 \
-DGRPC_HAVE_MSG_NOSIGNAL=1 \
-DGRPC_HAVE_UNIX_SOCKET=1 \
-DGRPC_POSIX_FORK=1 \
-DGRPC_POSIX_NO_SPECIAL_WAKEUP_FD=1 \
-DGRPC_POSIX_SOCKET=1 \
-DGRPC_POSIX_SOCKETUTILS=1 \
-DGRPC_POSIX_WAKEUP_FD=1 \
-DGRPC_LINUX_EPOLL=1 \
-DGRPC_POSIX_SOCKET_ARES_EV_DRIVER=1 \
-DGRPC_POSIX_SOCKET_EV=1 \
-DGRPC_POSIX_SOCKET_EV_EPOLLEX=1 \
-DGRPC_POSIX_SOCKET_EV_POLL=1 \
-DGRPC_POSIX_SOCKET_EV_EPOLL1=1 \
-DGRPC_POSIX_SOCKET_IF_NAMETOINDEX \
-DGRPC_POSIX_SOCKET_IOMGR=1 \
-DGRPC_POSIX_SOCKET_RESOLVE_ADDRESS=1 \
-DGRPC_POSIX_SOCKET_SOCKADDR=1 \
-DGRPC_POSIX_SOCKET_SOCKET_FACTORY=1 \
-DGRPC_POSIX_SOCKET_TCP=1 \
-DGRPC_POSIX_SOCKET_TCP_CLIENT=1 \
-DGRPC_POSIX_SOCKET_TCP_SERVER=1 \
-DGRPC_POSIX_SOCKET_TCP_SERVER_UTILS_COMMON=1 \
-DGRPC_POSIX_SOCKET_UDP_SERVER=1 \
-DGRPC_POSIX_SOCKET_UTILS_COMMON=1 \
-DGRPC_GETHOSTNAME_FALLBACK=1"

export PREFIX="/usr/local"

curl -sL "https://github.com/grpc/grpc/archive/v${GRPC_VERSION}.tar.gz" -o grpc-${GRPC_VERSION}.tar.gz
tar xf grpc-${GRPC_VERSION}.tar.gz
pushd grpc-${GRPC_VERSION}

cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=${PREFIX} \
      -DCMAKE_PREFIX_PATH=${PREFIX} \
      -DBUILD_SHARED_LIBS=OFF \
      -DCMAKE_C_FLAGS="${CFLAGS}" \
      -DCMAKE_CXX_FLAGS="${CFLAGS}" \
      -DgRPC_BUILD_CSHARP_EXT=OFF \
      -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF \
      -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
      -DgRPC_ABSL_PROVIDER=package \
      -DgRPC_CARES_PROVIDER=package \
      -DgRPC_GFLAGS_PROVIDER=package \
      -DgRPC_PROTOBUF_PROVIDER=package \
      -DgRPC_SSL_PROVIDER=package \
      -DgRPC_ZLIB_PROVIDER=package \
      -DOPENSSL_USE_STATIC_LIBS=ON \
      -GNinja .
ninja install
popd
rm -rf grpc-${GRPC_VERSION}.tar.gz grpc-${GRPC_VERSION}
