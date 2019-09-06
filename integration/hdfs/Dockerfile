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

FROM arrow:python-3.6

# installing libhdfs (JNI)
ARG HADOOP_VERSION=2.9.2
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    HADOOP_HOME=/usr/local/hadoop \
    HADOOP_OPTS=-Djava.library.path=/usr/local/hadoop/lib/native \
    PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
RUN apt-get update -y && \
    apt-get install -y --no-install-recommends openjdk-8-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q -O hadoop-$HADOOP_VERSION.tar.gz "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" && \
    tar -zxf /hadoop-$HADOOP_VERSION.tar.gz && \
    rm /hadoop-$HADOOP_VERSION.tar.gz && \
    mv /hadoop-$HADOOP_VERSION /usr/local/hadoop
COPY integration/hdfs/hdfs-site.xml $HADOOP_HOME/etc/hadoop/

# installing libhdfs3, it needs to be pinned, see ARROW-1465 and ARROW-1445
# after the conda-forge migration it's failing with abi incompatibilities, so
# turning it off for now
# RUN conda install -y -c conda-forge libhdfs3=2.2.31 && \
#     conda clean --all

# build cpp with tests
ENV CC=gcc \
    CXX=g++ \
    ARROW_ORC=ON \
    ARROW_HDFS=ON \
    ARROW_PYTHON=ON \
    ARROW_BUILD_TESTS=ON

# build and test
CMD ["/bin/bash", "-c", "arrow/ci/docker_build_cpp.sh && \
    arrow/ci/docker_build_python.sh && \
    arrow/integration/hdfs/runtest.sh"]
