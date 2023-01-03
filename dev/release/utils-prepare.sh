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
      ;;
    snapshot)
      local version=${next_version}-SNAPSHOT
      local r_version=${base_version}.9000
      ;;
  esac
  local major_version=${version%%.*}

  pushd "${ARROW_DIR}/c_glib"
  sed -i.bak -E -e \
    "s/^version = '.+'/version = '${version}'/" \
    meson.build
  rm -f meson.build.bak
  git add meson.build
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
    "s/\"version-string\": \".+\"/\"version-string\": \"${version}\"/" \
    vcpkg.json
  rm -f vcpkg.json.bak
  git add vcpkg.json
  popd

  pushd "${ARROW_DIR}/java"
  mvn versions:set -DnewVersion=${version}
  find . -type f -name pom.xml.versionsBackup -delete
  git add "pom.xml"
  git add "**/pom.xml"
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
    "s/arrow-[0-9.]+[0-9]+/arrow-${r_version}/g" \
    autobrew/apache-arrow.rb
  rm -f autobrew/apache-arrow.rb.bak
  git add autobrew/apache-arrow.rb
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
    "s/^default_version = '.+'/default_version = '${version}'/" \
    setup.py
  rm -f setup.py.bak
  git add setup.py
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
  popd

  pushd "${ARROW_DIR}/ruby"
  sed -i.bak -E -e \
    "s/^  VERSION = \".+\"/  VERSION = \"${version}\"/g" \
    */*/*/version.rb
  rm -f */*/*/version.rb.bak
  git add */*/*/version.rb
  popd

  pushd "${ARROW_DIR}/go"
  find . "(" -name "*.go*" -o -name "go.mod" ")" -exec sed -i.bak -E -e \
    "s|(github\\.com/apache/arrow/go)/v[0-9]+|\1/v${major_version}|" {} \;
  # update parquet writer version
  sed -i.bak -E -e \
    "s/\"parquet-go version .+\"/\"parquet-go version ${version}\"/" \
    parquet/writer_properties.go
  sed -i.bak -E -e \
    "s/const PkgVersion = \".*/const PkgVersion = \"${version}\"/" \
    arrow/doc.go

  find . -name "*.bak" -exec rm {} \;
  git add .
  popd

  pushd "${ARROW_DIR}"
  ${PYTHON:-python3} "dev/release/utils-update-docs-versions.py" \
                     . \
                     "${base_version}" \
                     "${next_version}"
  git add docs/source/_static/versions.json
  git add r/pkgdown/assets/versions.json
  popd
}
