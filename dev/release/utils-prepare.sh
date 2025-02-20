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

ARROW_DIR="${SOURCE_DIR}/../.."

update_versions() {
  local base_version=$1
  local next_version=$2
  local type=$3

  case ${type} in
    release)
      local version=${base_version}
      local r_version=${base_version}
      local python_version=${base_version}
      ;;
    snapshot)
      local version=${next_version}-SNAPSHOT
      local r_version=${base_version}.9000
      local python_version=${next_version}a0
      ;;
  esac
  local major_version=${version%%.*}

  pushd "${ARROW_DIR}/c_glib"
  sed -i.bak -E -e \
    "s/^    version: '.+'/    version: '${version}'/" \
    meson.build
  rm -f meson.build.bak
  git add meson.build

  # Add a new version entry only when the next release is a new major
  # release and it doesn't exist yet.
  if [ "${type}" = "snapshot" ] && \
     [ "${next_version}" = "${major_version}.0.0" ] && \
     ! grep -q -F "(${major_version}, 0)" tool/generate-version-header.py; then
    sed -i.bak -E -e \
      "s/^ALL_VERSIONS = \[$/&\\n        (${major_version}, 0),/" \
      tool/generate-version-header.py
    rm -f tool/generate-version-header.py.bak
    git add tool/generate-version-header.py
  fi

  sed -i.bak -E -e \
    "s/\"version-string\": \".+\"/\"version-string\": \"${version}\"/" \
    vcpkg.json
  rm -f vcpkg.json.bak
  git add vcpkg.json
  popd

  pushd "${ARROW_DIR}/ci/scripts"
  sed -i.bak -E -e \
    "s/^pkgver=.+/pkgver=${r_version}/" \
    PKGBUILD
  rm -f PKGBUILD.bak
  git add PKGBUILD
  popd

  pushd "${ARROW_DIR}/cpp"
  sed -i.bak -E -e \
    "s/^set\(ARROW_VERSION \".+\"\)/set(ARROW_VERSION \"${version}\")/" \
    CMakeLists.txt
  rm -f CMakeLists.txt.bak
  git add CMakeLists.txt

  sed -i.bak -E -e \
    "s/^    version: '.+'/    version: '${version}'/" \
    meson.build
  rm -f meson.build.bak
  git add meson.build

  sed -i.bak -E -e \
    "s/\"version-string\": \".+\"/\"version-string\": \"${version}\"/" \
    vcpkg.json
  rm -f vcpkg.json.bak
  git add vcpkg.json
  popd

  pushd "${ARROW_DIR}/csharp"
  sed -i.bak -E -e \
    "s/^    <Version>.+<\/Version>/    <Version>${version}<\/Version>/" \
    Directory.Build.props
  rm -f Directory.Build.props.bak
  git add Directory.Build.props
  popd

  pushd "${ARROW_DIR}/dev/tasks/homebrew-formulae"
  sed -i.bak -E -e \
    "s/arrow-[0-9.\-]+[0-9SNAPHOT]+/arrow-${version}/g" \
    apache-arrow-glib.rb \
    apache-arrow.rb
  rm -f \
    apache-arrow-glib.rb.bak \
    apache-arrow.rb.bak
  git add \
    apache-arrow-glib.rb \
    apache-arrow.rb
  popd

  pushd "${ARROW_DIR}/js"
  sed -i.bak -E -e \
    "s/^  \"version\": \".+\"/  \"version\": \"${version}\"/" \
    package.json
  rm -f package.json.bak
  git add package.json
  popd

  pushd "${ARROW_DIR}/matlab"
  sed -i.bak -E -e \
    "s/^set\(MLARROW_VERSION \".+\"\)/set(MLARROW_VERSION \"${version}\")/" \
    CMakeLists.txt
  rm -f CMakeLists.txt.bak
  git add CMakeLists.txt
  popd

  pushd "${ARROW_DIR}/python"
  sed -i.bak -E -e \
    "s/^fallback_version = '.+'/fallback_version = '${python_version}'/" \
    pyproject.toml
  rm -f pyproject.toml.bak
  git add pyproject.toml
  sed -i.bak -E -e \
    "s/^set\(PYARROW_VERSION \".+\"\)/set(PYARROW_VERSION \"${version}\")/" \
    CMakeLists.txt
  rm -f CMakeLists.txt.bak
  git add CMakeLists.txt
  popd

  pushd "${ARROW_DIR}/r"
  sed -i.bak -E -e \
    "s/^Version: .+/Version: ${r_version}/" \
    DESCRIPTION
  rm -f DESCRIPTION.bak
  git add DESCRIPTION

  # Replace dev version with release version
  sed -i.bak -E -e \
    "/^<!--/,/^# arrow /s/^# arrow .+/# arrow ${base_version}/" \
    NEWS.md
  if [ ${type} = "snapshot" ]; then
    # Add a news entry for the new dev version
    sed -i.bak -E -e \
      "/^<!--/,/^# arrow /s/^(# arrow .+)/# arrow ${r_version}\n\n\1/" \
      NEWS.md
  fi
  rm -f NEWS.md.bak
  git add NEWS.md

  # godoc link must reference current version, will reference v0.0.0 (2018) otherwise
  sed -i.bak -E -e \
    "s|(github\\.com/apache/arrow/go)/v[0-9]+|\1/v${major_version}|g" \
    _pkgdown.yml
  rm -f _pkgdown.yml.bak
  git add _pkgdown.yml
  popd

  pushd "${ARROW_DIR}/ruby"
  sed -i.bak -E -e \
    "s/^  VERSION = \".+\"/  VERSION = \"${version}\"/g" \
    */*/*/version.rb
  rm -f */*/*/version.rb.bak
  git add */*/*/version.rb
  popd

  pushd "${ARROW_DIR}/docs/source"
  # godoc link must reference current version, will reference v0.0.0 (2018) otherwise
  sed -i.bak -E -e \
    "s|(github\\.com/apache/arrow/go)/v[0-9]+|\1/v${major_version}|g" \
    index.rst
  rm -f index.rst.bak
  git add index.rst
  popd

  pushd "${ARROW_DIR}"
  ${PYTHON:-python3} "dev/release/utils-update-docs-versions.py" \
                     . \
                     "${base_version}" \
                     "${next_version}"
  git add docs/source/_static/versions.json
  git add r/pkgdown/assets/versions.html
  git add r/pkgdown/assets/versions.json
  popd
}

current_version() {
  grep ARROW_VERSION "${ARROW_DIR}/cpp/CMakeLists.txt" | \
    head -n1 | \
    grep -E -o '([0-9]+\.[0-9]+\.[0-9]+)'
}

so_version() {
  local version=$1
  local major_version=$(echo ${version} | cut -d. -f1)
  local minor_version=$(echo ${version} | cut -d. -f2)
  expr ${major_version} \* 100 + ${minor_version}
}

update_deb_package_names() {
  local version=$1
  local next_version=$2
  echo "Updating .deb package names for ${next_version}"
  deb_lib_suffix=$(so_version ${version})
  next_deb_lib_suffix=$(so_version ${next_version})
  if [ "${deb_lib_suffix}" != "${next_deb_lib_suffix}" ]; then
    pushd ${ARROW_DIR}/dev/tasks/linux-packages/apache-arrow
    for target in debian*/lib*${deb_lib_suffix}.install; do
      git mv \
        ${target} \
        $(echo ${target} | sed -e "s/${deb_lib_suffix}/${next_deb_lib_suffix}/")
    done
    deb_lib_suffix_substitute_pattern="s/(lib(arrow|gandiva|parquet)[-a-z]*)${deb_lib_suffix}/\\1${next_deb_lib_suffix}/g"
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" debian*/control*
    rm -f debian*/control*.bak
    git add debian*/control*
    popd

    pushd ${ARROW_DIR}/dev/release
    sed -i.bak -E -e "${deb_lib_suffix_substitute_pattern}" rat_exclude_files.txt
    rm -f rat_exclude_files.txt.bak
    git add rat_exclude_files.txt
    git commit -m "MINOR: [Release] Update .deb package names for ${next_version}"
    popd
  fi
}

update_linux_packages() {
  local version=$1
  local release_time=$2
  echo "Updating .deb/.rpm changelogs for ${version}"
  pushd ${ARROW_DIR}/dev/tasks/linux-packages
  rake \
    version:update \
    ARROW_RELEASE_TIME="${release_time}" \
    ARROW_VERSION=${version}
  git add */debian*/changelog */yum/*.spec.in
  git commit -m "MINOR: [Release] Update .deb/.rpm changelogs for ${version}"
  popd
}
