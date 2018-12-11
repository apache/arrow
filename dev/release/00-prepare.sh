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
set -e

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -eq 2 ]; then
  version=$1
  nextVersion=$2
  nextVersionSNAPSHOT=${nextVersion}-SNAPSHOT
  tag=apache-arrow-${version}

  echo "Updating changelog for $version"
  # Update changelog
  $SOURCE_DIR/update-changelog.sh $version

  echo "Updating .deb/.rpm changelogs for $version"
  cd $SOURCE_DIR/../tasks/linux-packages
  rake \
    version:update \
    ARROW_RELEASE_TIME="$(date +%Y-%m-%dT%H:%M:%S%z)" \
    ARROW_VERSION=${version}
  git add debian*/changelog yum/*.spec.in
  git commit -m "[Release] Update .deb/.rpm changelogs for $version"
  cd -

  echo "prepare release ${version} on tag ${tag} then reset to version ${nextVersionSNAPSHOT}"

  cd "${SOURCE_DIR}/../../java"

  mvn release:clean
  mvn release:prepare -Dtag=${tag} -DreleaseVersion=${version} -DautoVersionSubmodules -DdevelopmentVersion=${nextVersionSNAPSHOT}

  cd -

  echo "Updating .deb package names for $nextVersion"
  deb_lib_suffix=$(echo $version | sed -r -e 's/^[0-9]+\.([0-9]+)\.[0-9]+$/\1/')
  next_deb_lib_suffix=$(echo $nextVersion | sed -r -e 's/^[0-9]+\.([0-9]+)\.[0-9]+$/\1/')
  cd $SOURCE_DIR/../tasks/linux-packages/
  for target in debian*/lib*${deb_lib_suffix}.install; do
    git mv \
      ${target} \
      $(echo $target | sed -e "s/${deb_lib_suffix}/${next_deb_lib_suffix}/")
  done
  deb_lib_suffix_substitute_pattern="s/(lib(arrow|parquet)[-a-z]*)${deb_lib_suffix}/\\1${next_deb_lib_suffix}/g"
  sed -i.bak -r -e "${deb_lib_suffix_substitute_pattern}" debian*/control
  rm -f debian*/control.bak
  git add debian*/control
  cd -
  cd $SOURCE_DIR/../tasks/
  sed -i.bak -r -e "${deb_lib_suffix_substitute_pattern}" tasks.yml
  rm -f tasks.yml.bak
  git add tasks.yml
  cd -
  cd $SOURCE_DIR
  sed -i.bak -r -e "${deb_lib_suffix_substitute_pattern}" rat_exclude_files.txt
  rm -f rat_exclude_files.txt.bak
  git add rat_exclude_files.txt
  git commit -m "[Release] Update .deb package names for $nextVersion"
  cd -

  echo "prepare release ${version} in Rust crate"

  cd "${SOURCE_DIR}/../../rust"
  sed -i.bak -r -e "s/version = \"$version\"/version = \"$nextVersion\"/g" Cargo.toml
  rm -f Cargo.toml.bak
  git add Cargo.toml
  git commit -m "[Release] Update Rust Cargo.toml version for $nextVersion"
  cd -

  echo "Finish staging binary artifacts by running: sh dev/release/01-perform.sh"

else
  echo "Usage: $0 <version> <nextVersion>"
  exit
fi
