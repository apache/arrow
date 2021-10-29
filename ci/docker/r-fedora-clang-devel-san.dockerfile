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

# Fedora-clang-devel with the sanitizer enabled, this should/will
# be upstreamed to rhub, so separated out like this

# start with the Docker 'base R' Debian-based image
FROM rhub/fedora-clang:latest

# TODO: rhub maintainer when we upstream

ENV CRAN http://cran.r-project.org

RUN cd /tmp \
    && svn co https://svn.r-project.org/R/trunk R-devel

ENV RPREFIX /opt/R-devel

ENV ROPTIONS --with-x --with-recommended-packages --enable-R-shlib --enable-R-static-lib

ENV CC /usr/bin/clang
ENV CXX /usr/bin/clang++
ENV F77 gfortran
ENV CPP cpp

RUN yum -y install rsync
RUN dnf install -y libcxx-devel

RUN cd /tmp/R-devel \
    && ./tools/rsync-recommended \
    && R_PAPERSIZE=letter \
    R_BATCHSAVE="--no-save --no-restore" \
    CC="clang -fsanitize=address,undefined -fno-sanitize=float-divide-by-zero -fno-omit-frame-pointer" \
    CXX="clang++ -stdlib=libc++ -fsanitize=address,undefined -fno-sanitize=float-divide-by-zero -fno-omit-frame-pointer" \
    CFLAGS="-g -O3 -Wall -pedantic -mtune=native" \
    FFLAGS="-g -O2 -mtune=native" \
    FCFLAGS="-g -O2 -mtune=native" \
    CXXFLAGS="-g -O3 -Wall -pedantic -mtune=native" \
    MAIN_LD="clang++ -stdlib=libc++ -fsanitize=undefined,address" \
    R_OPENMP_CFLAGS=-fopenmp \
    ./configure --prefix=${RPREFIX} ${ROPTIONS} \
    && make \
    && make install

# TODO: re-enable when upstreamed?
# COPY xvfb-run /usr/local/bin/xvfb-run

# RUN chmod +x /usr/local/bin/xvfb-run && \
#     rm -f /bin/xvfb-run /usr/bin/xvfb-run

ENV RHUB_PLATFORM linux-x86_64-fedora-clang

# More verbose UBSAN/SAN output (cf #3) -- this is still somewhat speculative
# Entry copied from Prof Ripley's setup described at http://www.stats.ox.ac.uk/pub/bdr/memtests/README.txt
ENV ASAN_OPTIONS 'alloc_dealloc_mismatch=0:detect_leaks=0:detect_odr_violation=0'

ENV PATH=${RPREFIX}/bin:$PATH

RUN cd $RPREFIX/bin \
	&& mv R Rdevel \
	&& cp Rscript Rscriptdevel \
	&& ln -s Rdevel RDsan \
	&& ln -s Rscriptdevel RDscriptsan
