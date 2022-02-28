#!/usr/bin/env bash
#
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
#
# This script extracts dependency information of all Abseil
# packages in the given build folder.
#
# Arguments:
#   $1 - Path to a build folder

function join { local IFS="$1"; shift; echo "$*"; }

ABSL_PKGCONFIG_FOLDERS=$(find $1 -type f -name 'absl_*.pc' | sed -r 's|/[^/]+$||' | sort -u)
ABSL_LIBS=$(find $1 -type f -name 'libabsl_*.a' | sed -e 's/.*libabsl_//' -e 's/.a$//' | sort -u)
ABSL_INTERFACE_LIBS=$(find $1* -type f -name 'absl_*.pc' | sed -e 's/.*absl_//' -e 's/.pc$//' | sort -u)
ABSL_INTERFACE_LIBS=$(comm -13 <(echo ABSL_LIBS) <(echo $ABSL_INTERFACE_LIBS))

GRPC_PKGCONFIG_FOLDERS=$(find $1/grpc_* -type f -name '*.pc' | sed -r 's|/[^/]+$||' | sort -u)
GRPC_LIBS=$(find $1/grpc_* -type f -name '*.a' | sed -e 's/.*lib//' -e 's/.a$//' | sort -u)
GRPC_INTERFACE_LIBS=$(find $1/grpc_* -type f -name '*.pc' | sed -e 's/.*pkgconfig\///' -e 's/.pc$//' | sort -u)
GRPC_INTERFACE_LIBS=$(comm -13 <(echo $GRPC_LIBS) <(echo $GRPC_INTERFACE_LIBS))

printf "\n# Abseil produces the following libraries, each is fairly small, but there
# are (as you can see), many of them. We need to add the libraries first,
# and then describe how they depend on each other.\n\n"
echo "set(_ABSL_LIBS" $ABSL_LIBS")"

printf "\n# Abseil creates a number of header-only targets, which are needed to resolve dependencies.\n\n"
echo "set(_ABSL_INTERFACE_LIBS" $(join " "  ${ABSL_INTERFACE_LIBS[*]})")"
printf "\n"

for FOLDER in $ABSL_PKGCONFIG_FOLDERS
do
    grep Requires "$FOLDER"/*.pc |
    sed -e 's;.*/absl_;set_property(TARGET absl::;' \
        -e 's/.pc:Requires:/\n             PROPERTY INTERFACE_LINK_LIBRARIES /' \
        -e 's/ = 20210324,//g' \
        -e 's/ = 20210324//g' \
        -e 's/absl_/\n\                      absl::/g' \
        -e 's/$/)/' | \
    grep -v 'INTERFACE_LINK_LIBRARIES[ ]*)'
done

printf "\n\n"

echo "set(_GRPC_LIBS" $GRPC_LIBS")"
echo "set(_GRPC_INTERFACE_LIBS" $(join " "  ${GRPC_INTERFACE_LIBS[*]})")"

for FOLDER in $GRPC_PKGCONFIG_FOLDERS
do
    grep Requires "$FOLDER"/*.pc |
    sed -e 's/ / gRPC::/g' \
        -e 's;.*pkgconfig\/;set_property(TARGET gRPC::;' \
        -e 's/.pc:Requires:/\n             PROPERTY INTERFACE_LINK_LIBRARIES /'
done