#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM arrow:python-3.6

# installing java and maven
ARG MAVEN_VERSION=3.5.4
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    MAVEN_HOME=/usr/local/maven \
    M2_HOME=/root/.m2 \
    PATH=/root/.m2/bin:/usr/local/maven/bin:$PATH
RUN apt-get update -q -y && \
    apt-get install -q -y --no-install-recommends openjdk-8-jdk && \
    wget -q -O maven-$MAVEN_VERSION.tar.gz "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz" && \
    tar -zxf /maven-$MAVEN_VERSION.tar.gz && \
    rm /maven-$MAVEN_VERSION.tar.gz && \
    mv /apache-maven-$MAVEN_VERSION /usr/local/maven \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# installing specific version of spark
ARG SPARK_VERSION=master
RUN wget -q -O /tmp/spark.tar.gz https://github.com/apache/spark/archive/$SPARK_VERSION.tar.gz && \
    mkdir /spark && \
    tar -xzf /tmp/spark.tar.gz -C /spark --strip-components=1 && \
    rm /tmp/spark.tar.gz

# build cpp with tests
ENV CC=gcc \
    CXX=g++ \
    ARROW_PYTHON=ON \
    ARROW_HDFS=ON \
    ARROW_BUILD_TESTS=OFF

# build and test
CMD ["/bin/bash", "-c", "arrow/ci/docker_build_cpp.sh && \
    arrow/ci/docker_build_python.sh && \
    arrow/ci/docker_build_java.sh && \
    arrow/integration/spark/runtest.sh"]
