#!/usr/bin/env bash
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

set -eu

PWD="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ARROW_SRC=$(realpath "${PWD}/..")

: ${DOCKER_COMPOSE:="docker-compose"}
: ${DOCKER_COMPOSE_CONF:="${ARROW_SRC}/docker-compose.yml"}

docker_compose() {
  ${DOCKER_COMPOSE} -f ${DOCKER_COMPOSE_CONF} $@
}

main() {
  docker_compose build $@
  docker_compose run --rm $@
}

if [ $# -lt 1 ]; then
    echo "Usage: $0 <docker-compose-service>" >&2
    exit 1
fi

main $@
