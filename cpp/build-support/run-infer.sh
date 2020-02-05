#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
# Runs infer in the given directory
# Arguments:
#   $1 - Path to the infer binary
#   $2 - Path to the compile_commands.json to use
#   $3 - Apply infer step (1=capture, 2=analyze, 3=report)
#
INFER=$1
shift
COMPILE_COMMANDS=$1
shift
APPLY_STEP=$1
shift

if [ "$APPLY_STEP" == "1" ]; then
  $INFER capture --compilation-database $COMPILE_COMMANDS
  echo ""
  echo "Run 'make infer-analyze' next."
elif [ "$APPLY_STEP" == "2" ]; then
  # infer's analyze step can take a very long time to complete
  $INFER analyze
  echo ""
  echo "Run 'make infer-report' next."
  echo "See: http://fbinfer.com/docs/steps-for-ci.html"
elif [ "$APPLY_STEP" == "3" ]; then
  $INFER report --issues-csv ./infer-out/report.csv 1> /dev/null
  $INFER report --issues-txt ./infer-out/report.txt 1> /dev/null
  $INFER report --issues-json ./infer-out/report.json 1> /dev/null
  echo ""
  echo "Reports (report.txt, report.csv, report.json) can be found in the infer-out subdirectory."
else
  echo ""
  echo "See: http://fbinfer.com/docs/steps-for-ci.html"
fi
