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

# Turn .dockerignore to .dockerallow by excluding everything and explicitly
# allowing specific files and directories. This enables us to quickly add
# dependency files to the docker content without scanning the whole directory.
# This setup requires to all of our docker containers have arrow's source
# as a mounted directory.

ARG RELEASE_FLAG=--release
FROM ballistacompute/rust-base:0.4.0-20210213 AS base
WORKDIR /tmp/ballista
RUN apt-get -y install cmake
RUN cargo install cargo-chef 

FROM base as planner
COPY rust .
RUN cargo chef prepare --recipe-path recipe.json

FROM base as cacher
COPY --from=planner /tmp/ballista/recipe.json recipe.json
RUN cargo chef cook $RELEASE_FLAG --recipe-path recipe.json

FROM base as builder
COPY rust .
COPY --from=cacher /tmp/ballista/target target
ARG RELEASE_FLAG=--release

# force build.rs to run to generate configure_me code.
ENV FORCE_REBUILD='true'
RUN cargo build $RELEASE_FLAG

# put the executor on /executor (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/ballista-executor /executor; else mv /tmp/ballista/target/release/ballista-executor /executor; fi

# put the scheduler on /scheduler (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/ballista-scheduler /scheduler; else mv /tmp/ballista/target/release/ballista-scheduler /scheduler; fi

# put the tpch on /tpch (need to be copied from different places depending on FLAG)
ENV RELEASE_FLAG=${RELEASE_FLAG}
RUN if [ -z "$RELEASE_FLAG" ]; then mv /tmp/ballista/target/debug/tpch /tpch; else mv /tmp/ballista/target/release/tpch /tpch; fi

# Copy the binary into a new container for a smaller docker image
FROM ballistacompute/rust-base:0.4.0-20210213

COPY --from=builder /executor /

COPY --from=builder /scheduler /

COPY --from=builder /tpch /

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/executor", "--local"]
