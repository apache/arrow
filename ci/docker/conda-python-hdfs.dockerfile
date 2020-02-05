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

ARG repo
ARG arch=amd64
ARG python=3.6
FROM ${repo}:${arch}-conda-python-${python}

ARG jdk=8
ARG maven=3.5
RUN conda install -q \
        pandas \
        openjdk=${jdk} \
        maven=${maven} && \
    conda clean --all

# installing libhdfs (JNI)
ARG hdfs=2.9.2
ENV HADOOP_HOME=/opt/hadoop-${hdfs} \
    HADOOP_OPTS=-Djava.library.path=/opt/hadoop-${hdfs}/lib/native \
    PATH=$PATH:/opt/hadoop-${hdfs}/bin:/opt/hadoop-${hdfs}/sbin
RUN wget -q -O - "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=hadoop/common/hadoop-${hdfs}/hadoop-${hdfs}.tar.gz" | tar -xzf - -C /opt
COPY ci/etc/hdfs-site.xml $HADOOP_HOME/etc/hadoop/

# build cpp with tests
ENV CC=gcc \
    CXX=g++ \
    ARROW_FLIGHT=OFF \
    ARROW_GANDIVA=OFF \
    ARROW_PLASMA=OFF \
    ARROW_PARQUET=ON \
    ARROW_ORC=OFF \
    ARROW_HDFS=ON \
    ARROW_PYTHON=ON \
    ARROW_BUILD_TESTS=ON
