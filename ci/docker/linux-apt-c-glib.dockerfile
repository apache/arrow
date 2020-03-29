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

RUN apt-get update -y -q && \
    apt-get install -y -q \
        python3 \
        python3-pip \
        gtk-doc-tools \
        libgirepository1.0-dev \
        libglib2.0-doc \
        luarocks \
        pkg-config \
        ruby-dev && \
    if [ "$(lsb_release --codename --short)" = "xenial" ]; then \
      apt-get install -y -q --no-install-recommends -t xenial-backports \
        ninja-build; \
    fi && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN luarocks install lgi

RUN pip3 install meson && \
    gem install bundler

COPY c_glib/Gemfile /arrow/c_glib/
RUN bundle install --gemfile /arrow/c_glib/Gemfile

ENV ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_INSTALL_NAME_RPATH=OFF
