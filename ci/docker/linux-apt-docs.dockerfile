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

ARG r=4.4
ARG jdk=11

ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

# See R install instructions at https://cloud.r-project.org/bin/linux/
RUN apt-get update -y && \
    apt-get install -y \
        apt-transport-https \
        dirmngr \
        gpg \
        lsb-release && \
    gpg --keyserver keyserver.ubuntu.com \
        --recv-key 95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7 && \
    gpg --export 95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7 | \
        gpg --no-default-keyring \
            --keyring /usr/share/keyrings/cran.gpg \
            --import - && \
    echo "deb [signed-by=/usr/share/keyrings/cran.gpg] https://cloud.r-project.org/bin/linux/$(lsb_release -is | tr 'A-Z' 'a-z') $(lsb_release -cs)-cran40/" | \
        tee /etc/apt/sources.list.d/cran.list && \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
        sed -i \
            -e 's/main$/main contrib non-free non-free-firmware/g' \
            /etc/apt/sources.list.d/debian.sources; \
    fi && \
    apt-get update -y && \
    apt-get install -y --no-install-recommends \
        autoconf-archive \
        automake \
        chromium \
        chromium-sandbox \
        curl \
        doxygen \
        gi-docgen \
        gobject-introspection \
        libcurl4-openssl-dev \
        libfontconfig1-dev \
        libfribidi-dev \
        libgirepository1.0-dev \
        libglib2.0-doc \
        libharfbuzz-dev \
        libtiff-dev \
        libtool \
        libxml2-dev \
        meson \
        ninja-build \
        nodejs \
        npm \
        nvidia-cuda-toolkit \
        openjdk-${jdk}-jdk-headless \
        pandoc \
        r-recommended=${r}* \
        r-base=${r}* \
        rsync \
        ruby-dev \
        sudo \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
       npm install -g yarn @mermaid-js/mermaid-cli

ENV JAVA_HOME=/usr/lib/jvm/java-${jdk}-openjdk-amd64

ARG maven=3.8.7
COPY ci/scripts/util_download_apache.sh /arrow/ci/scripts/
RUN /arrow/ci/scripts/util_download_apache.sh \
    "maven/maven-3/${maven}/binaries/apache-maven-${maven}-bin.tar.gz" /opt
ENV PATH=/opt/apache-maven-${maven}/bin:$PATH
RUN mvn -version

COPY c_glib/Gemfile /arrow/c_glib/
RUN gem install --no-document bundler && \
    bundle install --gemfile /arrow/c_glib/Gemfile

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
COPY ci/etc/rprofile /arrow/ci/etc/
RUN cat /arrow/ci/etc/rprofile >> $(R RHOME)/etc/Rprofile.site
# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R -s -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Renviron.site

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow && \
    R -e "install.packages('pkgdown')"

RUN useradd --user-group --create-home --groups audio,video arrow
RUN echo "arrow ALL=(ALL:ALL) NOPASSWD:ALL" | \
        EDITOR=tee visudo -f /etc/sudoers.d/arrow
USER arrow

COPY docs/requirements.txt /arrow/docs/
RUN sudo chown -R arrow: ${ARROW_PYTHON_VENV} && \
    python3 -m venv ${ARROW_PYTHON_VENV} && \
    . ${ARROW_PYTHON_VENV}/bin/activate && \
    pip install -r arrow/docs/requirements.txt

ENV ARROW_ACERO=ON \
    ARROW_AZURE=OFF \
    ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_COMPUTE=ON \
    ARROW_CSV=ON \
    ARROW_DATASET=ON \
    ARROW_FILESYSTEM=ON \
    ARROW_FLIGHT=ON \
    ARROW_GCS=ON \
    ARROW_GLIB_VAPI=false \
    ARROW_HDFS=ON \
    ARROW_JSON=ON \
    ARROW_S3=ON \
    ARROW_USE_GLOG=OFF \
    CMAKE_UNITY_BUILD=ON \
    RETICULATE_PYTHON_ENV=${ARROW_PYTHON_VENV}
