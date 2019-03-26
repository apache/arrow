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

FROM arrow:cpp

RUN apt-get update -y -q && \
    apt-get -q install --no-install-recommends -y \
        autoconf-archive \
        gobject-introspection \
        gtk-doc-tools \
        libgirepository1.0-dev \
        libglib2.0-dev \
        pkg-config \
        ruby-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY c_glib/Gemfile /arrow/c_glib/
RUN conda install meson=0.47.1 && \
    conda clean --all && \
    gem install bundler && \
    bundle install --gemfile arrow/c_glib/Gemfile

# build cpp
ENV ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_INSTALL_NAME_RPATH=OFF \
    LD_LIBRARY_PATH="${CONDA_PREFIX}/lib" \
    PKG_CONFIG_PATH="${CONDA_PREFIX}/lib/pkgconfig" \
    GI_TYPELIB_PATH="${CONDA_PREFIX}/lib/girepository-1.0"

# build, install and test
CMD ["/bin/bash", "-c", "arrow/ci/docker_build_cpp.sh && \
    arrow/ci/docker_build_c_glib.sh && \
    arrow/c_glib/test/run-test.rb"]
