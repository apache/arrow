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

ARG r=3.6
ARG jdk=8

RUN apt-get update -y && \
    apt-get install -y \
        dirmngr \
        apt-transport-https \
        software-properties-common && \
    apt-key adv \
        --keyserver keyserver.ubuntu.com \
        --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu '$(lsb_release -cs)'-cran35/' && \
    apt-get install -y \
        autoconf-archive \
        automake \
        curl \
        doxygen \
        gobject-introspection \
        gtk-doc-tools \
        libcurl4-openssl-dev \
        libgirepository1.0-dev \
        libglib2.0-doc \
        libtool \
        libxml2-dev \
        ninja-build \
        nvidia-cuda-toolkit \
        openjdk-${jdk}-jdk-headless \
        pandoc \
        r-base=${r}* \
        rsync \
        ruby-dev \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-${jdk}-openjdk-amd64

ARG maven=3.5.4
COPY ci/scripts/util_download_apache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/util_download_apache.sh \
    "maven/maven-3/${maven}/binaries/apache-maven-${maven}-bin.tar.gz" /opt
ENV PATH=/opt/apache-maven-${maven}/bin:$PATH
RUN mvn -version

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

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow && \
    R -e "install.packages('pkgdown')"

ENV ARROW_PYTHON=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_USE_GLOG=OFF \
