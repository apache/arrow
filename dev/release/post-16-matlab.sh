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

# Use verbose debug output.
set -ex
set -o pipefail

# Ensure that a version number has been supplied.
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <version>"
  exit
fi

# The version number to use will be the first input argument to the script.
version=$1

# Download the MATLAB MLTBX file from JFrog Artifactory to a temp directory.
tmp=$(mktemp -d -t "arrow-post-matlab.XXXXX")
base_url=https://apache.jfrog.io/artifactory/arrow/matlab/${version}
matlab_artifact="matlab-arrow-${version}.mltbx"
curl --fail --location --output ${tmp}/${matlab_artifact} ${base_url}/${matlab_artifact}
github_org="mathworks"
github_repo="arrow"
github_api_endpoint="https://api.github.com/repos/${github_org}/${github_repo}/releases"
github_api_params_template='{"tag_name":"%s","target_commitish":"main","name":"%s","body":"MATLAB Interface to Arrow version %s","draft":false,"prerelease":false,"generate_release_notes":false}'
github_api_params=$(printf "$github_api_params_template" "$version" "$version" "$version")

# Upload MLTBX file to GitHub releases.
curl -L \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_ACCESS_TOKEN}" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  ${github_api_endpoint} \
  -d "$github_api_params"

# Clean up downloaded artifacts.
rm -rf "${tmp}"

echo "Success! The released MATLAB packages are available here:"
echo "  https://github.com/${github_org}/${github_repo}/releases/${version}"
