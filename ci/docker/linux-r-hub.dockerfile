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

# Ensure parallel R package installation, set CRAN repo mirror,
# and use pre-built binaries where possible
COPY ci/scripts/rprofile /arrow/ci/scripts/
RUN cat /arrow/ci/scripts/rprofile >> $(R RHOME)/etc/Rprofile.site
# Also ensure parallel compilation of C/C++ code
RUN echo "MAKEFLAGS=-j$(R --slave -e 'cat(parallel::detectCores())')" >> $(R RHOME)/etc/Makeconf

COPY ci/scripts/r_deps.sh /arrow/ci/scripts/
COPY r/DESCRIPTION /arrow/r/
RUN /arrow/ci/scripts/r_deps.sh /arrow
