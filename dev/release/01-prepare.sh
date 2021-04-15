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
set -ue

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <version> <next_version> <rc-num>"
  exit 1
fi

update_versions() {
  local base_version=$1
  local next_version=$2
  local type=$3

  case ${type} in
    release)
      local version=${base_version}
      local r_version=${base_version}
      ;;
    snapshot)
      local version=${next_version}-SNAPSHOT
      local r_version=${base_version}.9000
      ;;
  esac

  cd "${SOURCE_DIR}/../../c_glib"
  sed -i.bak -E -e \
    "s/^version = '.+'/version = '${version}'/" \
    meson.build
  rm -f meson.build.bak
  git add meson.build
  cd -

  cd "${SOURCE_DIR}/../../ci/scripts"
  sed -i.bak -E -e \
    "s/^pkgver=.+/pkgver=${r_version}/" \
    PKGBUILD
  rm -f PKGBUILD.bak
  git add PKGBUILD
  cd -

  cd "${SOURCE_DIR}/../../cpp"
  sed -i.bak -E -e \
    "s/^set\(ARROW_VERSION \".+\"\)/set(ARROW_VERSION \"${version}\")/" \
    CMakeLists.txt
  rm -f CMakeLists.txt.bak
  git add CMakeLists.txt

  sed -i.bak -E -e \
    "s/\"version-string\": \".+\"/\"version-string\": \"${version}\"/" \
    vcpkg.json
  rm -f vcpkg.json.bak
  git add vcpkg.json
  cd -

  cd "${SOURCE_DIR}/../../csharp"
  sed -i.bak -E -e \
    "s/^    <Version>.+<\/Version>/    <Version>${version}<\/Version>/" \
    Directory.Build.props
  rm -f Directory.Build.props.bak
  git add Directory.Build.props
  cd -

  cd "${SOURCE_DIR}/../../dev/tasks/homebrew-formulae"
  sed -i.bak -E -e \
    "s/arrow-[0-9.]+[0-9]+/arrow-${r_version}/g" \
    autobrew/apache-arrow.rb
  rm -f autobrew/apache-arrow.rb.bak
  git add autobrew/apache-arrow.rb
  sed -i.bak -E -e \
    "s/arrow-[0-9.\-]+[0-9SNAPHOT]+/arrow-${version}/g" \
    apache-arrow.rb
  rm -f apache-arrow.rb.bak
  git add apache-arrow.rb
  cd -

  cd "${SOURCE_DIR}/../../js"
  sed -i.bak -E -e \
    "s/^  \"version\": \".+\"/  \"version\": \"${version}\"/" \
    package.json
  rm -f package.json.bak
  git add package.json
  cd -

  cd "${SOURCE_DIR}/../../matlab"
  sed -i.bak -E -e \
    "s/^set\(MLARROW_VERSION \".+\"\)/set(MLARROW_VERSION \"${version}\")/" \
    CMakeLists.txt
  rm -f CMakeLists.txt.bak
  git add CMakeLists.txt
  cd -

  cd "${SOURCE_DIR}/../../python"
  sed -i.bak -E -e \
    "s/^default_version = '.+'/default_version = '${version}'/" \
    setup.py
  rm -f setup.py.bak
  git add setup.py
  cd -

  cd "${SOURCE_DIR}/../../r"
  sed -i.bak -E -e \
    "s/^Version: .+/Version: ${r_version}/" \
    DESCRIPTION
  rm -f DESCRIPTION.bak
  git add DESCRIPTION
  if [ ${type} = "snapshot" ]; then
    # Add a news entry for the new dev version
    echo "dev"
    sed -i.bak -E -e \
      "0,/^# arrow /s/^(# arrow .+)/# arrow ${r_version}\n\n\1/" \
      NEWS.md
  else
    # Replace dev version with release version
    echo "release"
    sed -i.bak -E -e \
      "0,/^# arrow /s/^# arrow .+/# arrow ${r_version}/" \
      NEWS.md
  fi
  rm -f NEWS.md.bak
  git add NEWS.md
  cd -

  cd "${SOURCE_DIR}/../../ruby"
  sed -i.bak -E -e \
    "s/^  VERSION = \".+\"/  VERSION = \"${version}\"/g" \
    */*/*/version.rb
  rm -f */*/*/version.rb.bak
  git add */*/*/version.rb
  cd -

  cd "${SOURCE_DIR}/../../rust"
  sed -i.bak -E \
    -e "s/^version = \".+\"/version = \"${version}\"/g" \
    -e "s/^(arrow = .* version = )\".*\"(( .*)|(, features = .*)|(, optional = .*))$/\\1\"${version}\"\\2/g" \
    -e "s/^(arrow-flight = .* version = )\".+\"( .*)/\\1\"${version}\"\\2/g" \
    -e "s/^(parquet = .* version = )\".*\"(( .*)|(, features = .*))$/\\1\"${version}\"\\2/g" \
    -e "s/^(parquet_derive = .* version = )\".*\"(( .*)|(, features = .*))$/\\1\"${version}\"\\2/g" \
    */Cargo.toml
  rm -f */Cargo.toml.bak
  git add */Cargo.toml

  sed -i.bak -E \
    -e "s/^([^ ]+) = \".+\"/\\1 = \"${version}\"/g" \
    -e "s,docs\.rs/crate/([^/]+)/[^)]+,docs.rs/crate/\\1/${version},g" \
    */README.md
  rm -f */README.md.bak
  git add */README.md
  cd -
}

############################## Pre-Tag Commits ##############################

version=$1
next_version=$2
next_version_snapshot="${next_version}-SNAPSHOT"
rc_number=$3

release_tag="apache-arrow-${version}"
release_branch="release-${version}"
release_candidate_branch="release-${version}-rc${rc_number}"

: ${PREPARE_DEFAULT:=1}
: ${PREPARE_CHANGELOG:=${PREPARE_DEFAULT}}
: ${PREPARE_LINUX_PACKAGES:=${PREPARE_DEFAULT}}
: ${PREPARE_VERSION_PRE_TAG:=${PREPARE_DEFAULT}}
: ${PREPARE_BRANCH:=${PREPARE_DEFAULT}}
: ${PREPARE_TAG:=${PREPARE_DEFAULT}}
: ${PREPARE_VERSION_POST_TAG:=${PREPARE_DEFAULT}}
: ${PREPARE_DEB_PACKAGE_NAMES:=${PREPARE_DEFAULT}}

if [ ${PREPARE_TAG} -gt 0 ]; then
  if [ $(git tag -l "${release_tag}") ]; then
    echo "Delete existing git tag $release_tag"
    git tag -d "${release_tag}"
  fi
fi

if [ ${PREPARE_BRANCH} -gt 0 ]; then
  if [[ $(git branch -l "${release_candidate_branch}") ]]; then
    next_rc_number=$(($rc_number+1))
    echo "Branch ${release_candidate_branch} already exists, so create a new release candidate:"
    echo "1. Checkout the master branch for major releases and maint-<version> for patch releases."
    echo "2. Execute the script again with bumped RC number."
    echo "Commands:"
    echo "   git checkout master"
    echo "   dev/release/01-prepare.sh ${version} ${next_version} ${next_rc_number}"
    exit 1
  fi

  echo "Create local branch ${release_candidate_branch} for release candidate ${rc_number}"
  git checkout -b ${release_candidate_branch}
fi

############################## Pre-Tag Commits ##############################

if [ ${PREPARE_CHANGELOG} -gt 0 ]; then
  echo "Updating changelog for $version"
  # Update changelog
  archery release changelog add $version
  git add ${SOURCE_DIR}/../../CHANGELOG.md
  git commit -m "[Release] Update CHANGELOG.md for $version"
fi

if [ ${PREPARE_LINUX_PACKAGES} -gt 0 ]; then
  echo "Updating .deb/.rpm changelogs for $version"
  cd $SOURCE_DIR/../tasks/linux-packages
  rake \
    version:update \
    ARROW_RELEASE_TIME="$(date +%Y-%m-%dT%H:%M:%S%z)" \
    ARROW_VERSION=${version}
  git add */debian*/changelog */yum/*.spec.in
  git commit -m "[Release] Update .deb/.rpm changelogs for $version"
  cd -
fi

if [ ${PREPARE_VERSION_PRE_TAG} -gt 0 ]; then
  echo "Prepare release ${version} on tag ${release_tag} then reset to version ${next_version_snapshot}"

  update_versions "${version}" "${next_version}" "release"
  git commit -m "[Release] Update versions for ${version}"
fi

############################## Tag the Release ##############################

if [ ${PREPARE_TAG} -gt 0 ]; then
  git tag -a "${release_tag}" -m "[Release] Apache Arrow Release ${version}"
fi

############################## Post-Tag Commits #############################

if [ ${PREPARE_VERSION_POST_TAG} -gt 0 ]; then
  echo "Updating versions for ${next_version_snapshot}"
  update_versions "${version}" "${next_version}" "snapshot"
  git commit -m "[Release] Update versions for ${next_version_snapshot}"
fi

if [ ${PREPARE_DEB_PACKAGE_NAMES} -gt 0 ]; then
  echo "Updating .deb package names for ${next_version}"
  so_version() {
    local version=$1
    local major_version=$(echo $version | sed -E -e 's/^([0-9]+)\.[0-9]+\.[0-9]+$/\1/')
    local minor_version=$(echo $version | sed -E -e 's/^[0-9]+\.([0-9]+)\.[0-9]+$/\1/')
    expr ${major_version} \* 100 + ${minor_version}
  }
  deb_lib_suffix=$(so_version $version)
  next_deb_lib_suffix=$(so_version $next_version)
  if [ "${deb_lib_suffix}" != "${next_deb_lib_suffix}" ]; then
    cd $SOURCE_DIR/../tasks/linux-packages/apache-arrow
    for target in debian*/lib*${deb_lib_suffix}.install; do
      git mv \
	${target} \
	$(echo $target | sed -e "s/${deb_lib_suffix}/${next_deb_lib_suffix}/")
    done
    deb_lib_suffix_substitute_pattern="s/(lib(arrow|gandiva|parquet|plasma)[-a-z]*)${deb_lib_suffix}/\\1${next_deb_lib_suffix}/g"
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" debian*/control*
    rm -f debian*/control*.bak
    git add debian*/control*
    cd -
    cd $SOURCE_DIR/../tasks/
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" tasks.yml
    rm -f tasks.yml.bak
    git add tasks.yml
    cd -
    cd $SOURCE_DIR
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" rat_exclude_files.txt
    rm -f rat_exclude_files.txt.bak
    git add rat_exclude_files.txt
    git commit -m "[Release] Update .deb package names for $next_version"
    cd -
  fi
fi
