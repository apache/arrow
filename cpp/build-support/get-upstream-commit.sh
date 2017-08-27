#!/bin/bash
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
# Script which tries to determine the most recent git hash in the current
# branch which was checked in by gerrit. This commit hash is printed to
# stdout.
#
# It does so by looking for the 'Reviewed-on' tag added by gerrit. This is
# more foolproof than trying to guess at the "origin/" branch name, since the
# developer might be working on some local topic branch.
set -e

git log --grep='Reviewed-on: ' -n1 --pretty=format:%H
