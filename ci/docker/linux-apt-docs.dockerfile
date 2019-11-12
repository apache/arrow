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

ARG base
FROM ${base}

ARG jdk=8
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf-archive \
        automake \
        doxygen \
        gobject-introspection \
        gtk-doc-tools \
        libgirepository1.0-dev \
        libglib2.0-doc \
        libtool \
        ninja-build \
        openjdk-${jdk}-jdk-headless \
        ruby-dev \
        rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-${jdk}-openjdk-amd64

ARG maven=3.5.4
RUN wget -q -O - "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=maven/maven-3/${maven}/binaries/apache-maven-${maven}-bin.tar.gz" | tar -xzf - -C /opt
ENV PATH=/opt/apache-maven-${maven}/bin:$PATH

ARG node=11
RUN wget -q -O - https://deb.nodesource.com/setup_${node}.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install \
        meson \
        breathe \
        ipython \
        sphinx \
        sphinx_rtd_theme

COPY c_glib/Gemfile /arrow/c_glib/
RUN gem install bundler && \
    bundle install --gemfile /arrow/c_glib/Gemfile

ENV ARROW_PYTHON=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_USE_GLOG=OFF \
