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

OPTIONS=("machine" "benchmarks" "runs")

option=${1-help}
datafile=${2-machine.json}
uri=${3-localhost:5000/graphql}

help() {
  cat <<HELP
  Submit data via GraphQL
  
  Usage:
  ${0} [option] [JSON_file] [URI]

  Arguments:
    option    - $(echo ${OPTIONS[@]} | sed 's/ /|/g')
    JSON_file - path to the submission file (default 'machine.json')
    URI       - URI to submit to (default 'localhost:5000/graphql')
HELP
}

escape_quote() {  sed 's/"/\\"/g'; }

template() {
  cat <<TEMPLATE
  {
    "query": "mutation (\$jsonb: JSON!){${1}(input:{fromJsonb:\$jsonb}){${2}}}",
    "variables": {
      "jsonb": "$(echo $(cat ${datafile}) | escape_quote )"
    }
  }
TEMPLATE
}

submit () {
  curl -X POST -H "Content-Type: application/json"  --data @<(template $1 $2) ${uri}
}


case "$1" in
  machine)
    submit ingestMachineView integer;;

  benchmarks)
    submit ingestBenchmarkView integers;;

  runs)
    if grep -q context <(head -n2 ${2})
    then
      submit ingestBenchmarkRunsWithContext bigInts
    else
      submit ingestBenchmarkRunView bigInts
    fi;;

  *)
    help
    exit 1
esac
