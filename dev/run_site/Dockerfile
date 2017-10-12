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
#

FROM ubuntu:14.04
ADD . /apache-arrow
WORKDIR /apache-arrow
# Prerequsites for apt-add-repository
RUN apt-get update && apt-get install -y \
    software-properties-common python-software-properties
# Set up Ruby repository
RUN apt-add-repository ppa:brightbox/ruby-ng
# The publication tools
RUN apt-get update; apt-get install -y \
    apt-transport-https \
    ruby2.2-dev \
    ruby2.2 \
    zlib1g-dev \ 
    make \
    gcc
RUN gem install jekyll bundler
CMD arrow/dev/run_site/run_site.sh
