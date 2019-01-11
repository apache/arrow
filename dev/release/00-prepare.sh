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

update_versions() {
  local base_version=$1
  local next_version=$2
  local type=$3

  case ${type} in
    release)
      version=${base_version}
      r_version=${base_version}
      ;;
    snapshot)
      version=${next_version}-SNAPSHOT
      r_version=${base_version}.9000
      ;;
  esac

  cd "${SOURCE_DIR}/../../cpp"
  sed -i.bak -r -e \
    "s/^set\(ARROW_VERSION \".+\"\)/set(ARROW_VERSION \"${version}\")/" \
    CMakeLists.txt
  rm -f CMakeLists.txt.bak
  git add CMakeLists.txt
  cd -

  cd "${SOURCE_DIR}/../../c_glib"
  sed -i.bak -r -e \
    "s/^m4_define\(\[arrow_glib_version\], .+\)/m4_define([arrow_glib_version], ${version})/" \
    configure.ac
  sed -i.bak -r -e \
    "s/^version = '.+'/version = '${version}'/" \
    meson.build
  rm -f configure.ac.bak meson.build.bak
  git add configure.ac meson.build
  cd -

  # We can enable this when Arrow JS uses the same version.
  # cd "${SOURCE_DIR}/../../js"
  # sed -i.bak -r -e \
  #   "s/^  \"version\": \".+\"/  \"version\": \"${version}\"/" \
  #   package.json
  # rm -f package.json
  # git add package.json
  # cd -

  cd "${SOURCE_DIR}/../../matlab"
  sed -i.bak -r -e \
    "s/^set\(MLARROW_VERSION \".+\"\)/set(MLARROW_VERSION \"${version}\")/" \
    CMakeLists.txt
  rm -f CMakeLists.txt.bak
  git add CMakeLists.txt
  cd -

  cd "${SOURCE_DIR}/../../python"
  sed -i.bak -r -e \
    "s/^default_version: '.+'/default_version = '${version}'/" \
    setup.py
  rm -f setup.py.bak
  git add setup.py
  cd -

  cd "${SOURCE_DIR}/../../r"
  sed -i.bak -r -e \
    "s/^Version: .+/Version: ${r_version}/" \
    DESCRIPTION
  rm -f DESCRIPTION.bak
  git add DESCRIPTION
  cd -

  cd "${SOURCE_DIR}/../../ruby"
  sed -i.bak -r -e \
    "s/^  VERSION = \".+\"/  VERSION = \"${version}\"/g" \
    */*/*/version.rb
  rm -f */*/*/version.rb.bak
  git add */*/*/version.rb
  cd -

  cd "${SOURCE_DIR}/../../rust"
  sed -i.bak -r -e \
    "s/^version = \".+\"/version = \"${version}\"/g" \
    arrow/Cargo.toml parquet/Cargo.toml
  rm -f arrow/Cargo.toml.bak parquet/Cargo.toml.bak
  git add arrow/Cargo.toml parquet/Cargo.toml
  cd -
}

if [ "$#" -eq 2 ]; then
  ############################## Pre-Tag Commits ##############################

  version=$1
  next_version=$2
  next_version_snapshot=${next_version}-SNAPSHOT
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

  echo "prepare release ${version} on tag ${tag} then reset to version ${next_version_snapshot}"

  update_versions "${version}" "${next_version}" "release"
  git commit -m "[Release] Update versions for ${version}"

  cd "${SOURCE_DIR}/../../java"
  mvn release:clean
  mvn release:prepare -Dtag=${tag} -DreleaseVersion=${version} -DautoVersionSubmodules -DdevelopmentVersion=${next_version_snapshot}
  cd -

  ############################## Post-Tag Commits #############################

  echo "Updating versions for ${next_version_snapshot}"
  update_versions "${version}" "${next_version}" "snapshot"
  git commit -m "[Release] Update versions for ${next_version_snapshot}"

  echo "Updating .deb package names for ${next_version}"
  deb_lib_suffix=$(echo $version | sed -r -e 's/^[0-9]+\.([0-9]+)\.[0-9]+$/\1/')
  next_deb_lib_suffix=$(echo $next_version | sed -r -e 's/^[0-9]+\.([0-9]+)\.[0-9]+$/\1/')
  cd $SOURCE_DIR/../tasks/linux-packages/
  for target in debian*/lib*${deb_lib_suffix}.install; do
    git mv \
      ${target} \
      $(echo $target | sed -e "s/${deb_lib_suffix}/${next_deb_lib_suffix}/")
  done
  deb_lib_suffix_substitute_pattern="s/(lib(arrow|gandiva|parquet|plasma)[-a-z]*)${deb_lib_suffix}/\\1${next_deb_lib_suffix}/g"
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
  git commit -m "[Release] Update .deb package names for $next_version"
  cd -

  echo "Finish staging binary artifacts by running: sh dev/release/01-perform.sh"

else
  echo "Usage: $0 <version> <next_version>"
  exit
fi
