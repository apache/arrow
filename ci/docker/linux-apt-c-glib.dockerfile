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
        gtk-doc-tools \
        libgirepository1.0-dev \
        libglib2.0-doc \
        lsb-release \
        luarocks \
        ninja-build \
        pkg-config \
        python3 \
        python3-pip \
        ruby-dev \
        valac && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN luarocks install lgi

# pip on Ubuntu 20.04 may be buggy:
#
# Collecting meson
#  Downloading meson-0.53.2.tar.gz (1.6 MB)
#  Installing build dependencies: started
#  Installing build dependencies: finished with status 'done'
#  Getting requirements to build wheel: started
#  Getting requirements to build wheel: finished with status 'error'
#  ERROR: Command errored out with exit status 1:
#   command: /usr/bin/python3 /usr/share/python-wheels/pep517-0.7.0-py2.py3-none-any.whl/pep517/_in_process.py get_requires_for_build_wheel /tmp/tmpsk4jveay
#       cwd: /tmp/pip-install-jn79a_kh/meson
#  Complete output (1 lines):
#  /usr/bin/python3: can't find '__main__' module in '/usr/share/python-wheels/pep517-0.7.0-py2.py3-none-any.whl/pep517/_in_process.py'
#  ----------------------------------------
# ERROR: Command errored out with exit status 1: /usr/bin/python3 /usr/share/python-wheels/pep517-0.7.0-py2.py3-none-any.whl/pep517/_in_process.py get_requires_for_build_wheel /tmp/tmpsk4jveay Check the logs for full command output.
RUN (python3 -m pip install meson || \
         python3 -m pip install --no-use-pep517 meson) && \
    gem install --no-document bundler

COPY c_glib/Gemfile /arrow/c_glib/
RUN bundle install --gemfile /arrow/c_glib/Gemfile

ENV ARROW_BUILD_STATIC=OFF \
    ARROW_BUILD_TESTS=OFF \
    ARROW_BUILD_UTILITIES=OFF \
    ARROW_INSTALL_NAME_RPATH=OFF
