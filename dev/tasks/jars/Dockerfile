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
FROM quay.io/pypa/manylinux2014_x86_64:latest

# Install dependencies
RUN yum install -y xz java-1.8.0-openjdk-devel ccache flex wget && yum clean all

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk/

ADD docker-scripts/build_zlib.sh /
RUN /build_zlib.sh

ADD docker-scripts/build_perl.sh /
RUN /build_perl.sh

ADD docker-scripts/build_openssl.sh /
RUN /build_openssl.sh
ENV OPENSSL_HOME /usr/local

ADD docker-scripts/build_wget.sh /
RUN /build_wget.sh

# Install cmake manylinux1 package
ADD docker-scripts/install_cmake.sh /
RUN /install_cmake.sh

ADD docker-scripts/build_gflags.sh /
RUN /build_gflags.sh

ADD docker-scripts/build_protobuf.sh /
RUN /build_protobuf.sh
ENV PROTOBUF_HOME /usr/local

ADD docker-scripts/build_cares.sh /
RUN /build_cares.sh

ADD docker-scripts/build_absl.sh /
RUN /build_absl.sh

ADD docker-scripts/build_grpc.sh /
RUN /build_grpc.sh

ADD docker-scripts/build_boost.sh /
RUN /build_boost.sh

ADD docker-scripts/build_gtest.sh /
RUN /build_gtest.sh
ENV GTEST_HOME /usr/local

ADD docker-scripts/build_bison.sh /
RUN /build_bison.sh

ADD docker-scripts/build_thrift.sh /
RUN /build_thrift.sh
ENV THRIFT_HOME /usr/local

ADD docker-scripts/build_curl.sh /
RUN /build_curl.sh
ENV CURL_HOME /opt/curl

ADD docker-scripts/build_aws_sdk.sh /
RUN /build_aws_sdk.sh

ADD docker-scripts/build_brotli.sh /
RUN /build_brotli.sh
ENV BROTLI_HOME /usr/local

ADD docker-scripts/build_snappy.sh /
RUN /build_snappy.sh
ENV SNAPPY_HOME /usr/local

ADD docker-scripts/build_lz4.sh /
RUN /build_lz4.sh
ENV LZ4_HOME /usr/local

ADD docker-scripts/build_zstd.sh /
RUN /build_zstd.sh
ENV ZSTD_HOME /usr/local

ADD docker-scripts/build_ccache.sh /
RUN /build_ccache.sh

ADD docker-scripts/build_glog.sh /
RUN /build_glog.sh
ENV GLOG_HOME /usr/local

WORKDIR /
RUN git clone https://github.com/matthew-brett/multibuild.git && cd multibuild && git checkout 03950c9a7feb09d215f82d6563c4ffd91274a1e1

ADD docker-scripts/build_llvm.sh /
RUN /build_llvm.sh

RUN rm -rf /opt/_internal/cpython-2.7.*

ADD docker-scripts/build_rapidjson.sh /
RUN /build_rapidjson.sh

ADD docker-scripts/build_re2.sh /
RUN /build_re2.sh

ADD docker-scripts/build_bz2.sh /
RUN /build_bz2.sh

ADD docker-scripts/build_utf8proc.sh /
RUN /build_utf8proc.sh
ENV UTF8PROC_HOME /usr/local

ADD docker-scripts/build_orc.sh /
RUN /build_orc.sh
ENV ORC_HOME /usr/local