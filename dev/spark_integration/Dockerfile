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
#
FROM ubuntu:14.04
ADD . /apache-arrow
WORKDIR /apache-arrow
# Basic OS utilities
RUN apt-get update && apt-get install -y \
        wget \
        git \
        maven\
        software-properties-common
# Setup Java
RUN add-apt-repository ppa:openjdk-r/ppa
RUN apt-get update && apt-get install -y openjdk-8-jdk
update-java-alternatives -s java-1.8.0-openjdk-amd64
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin
# This will install conda in /home/ubuntu/miniconda
RUN wget -O /tmp/miniconda.sh \
    https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    bash /tmp/miniconda.sh -b -p /home/ubuntu/miniconda && \
    rm /tmp/miniconda.sh
# Create Conda environment
ENV PATH="/home/ubuntu/miniconda/bin:${PATH}"
RUN conda create -y -q -n test-environment \
    python=3.6
# Install dependencies
RUN conda install -c conda-forge \
    numpy \
    pandas \
    "pytest<=3.1.1"

CMD ["arrow/dev/spark_integration/spark_integration.sh"]

