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

ARG r=4.1
ARG jdk=8

# See R install instructions at https://cloud.r-project.org/bin/linux/ubuntu/
RUN apt-get update -y && \
    apt-get install -y \
        dirmngr \
        apt-transport-https \
        software-properties-common && \
    apt-key adv \
        --keyserver keyserver.ubuntu.com \
        --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu '$(lsb_release -cs)'-cran40/' && \
    apt-get install -y --no-install-recommends \
        autoconf-archive \
        automake \
        curl \
        doxygen \
        gobject-introspection \
        gtk-doc-tools \
        libcurl4-openssl-dev \
        libfontconfig1-dev \
        libfribidi-dev \
        libgirepository1.0-dev \
        libglib2.0-doc \
        libharfbuzz-dev \
        libtiff-dev \
        libtool \
        libxml2-dev \
        ninja-build \
        nvidia-cuda-toolkit \
        openjdk-${jdk}-jdk-headless \
        pandoc \
        r-recommended=${r}* \
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

ARG node=14
RUN wget -q -O - https://deb.nodesource.com/setup_${node}.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    npm install -g yarn

# ARROW-13353: breathe >= 4.29.1 tries to parse template arguments,
# but Sphinx can't parse constructs like `typename...`.
RUN pip install \
        meson \
        breathe==4.29.0 \
        ipython \
        sphinx \
        pydata-sphinx-theme

COPY c_glib/Gemfile /arrow/c_glib/
RUN gem install --no-document bundler && \
    bundle install --gemfile /arrow/c_glib/Gemfile

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
COPY ci/etc/rprofile /arrow/ci/etc/
RUN cat /arrow/ci/etc/rprofile >> $(R RHOME)/etc/Rprofile.site
# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Makeconf

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow && \
    R -e "install.packages('pkgdown')"

ENV ARROW_FLIGHT=ON \
    ARROW_PYTHON=ON \
    ARROW_S3=ON \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_USE_GLOG=OFF \
    CMAKE_UNITY_BUILD=ON \
