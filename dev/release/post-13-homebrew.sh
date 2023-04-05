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

set -ue

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <github-user>"
  exit 1
fi

version=$1
github_user=$2

url="https://www.apache.org/dyn/closer.lua?path=arrow/arrow-${version}/apache-arrow-${version}.tar.gz"
sha256="$(curl https://dist.apache.org/repos/dist/release/arrow/arrow-${version}/apache-arrow-${version}.tar.gz.sha256 | cut -d' ' -f1)"

pushd "$(brew --repository homebrew/core)"

if ! git remote | grep -q --fixed-strings ${github_user}; then
  echo "Setting ''${github_user}' remote"
  git remote add ${github_user} git@github.com:${github_user}/homebrew-core.git
fi

echo "Updating working copy"
git fetch --all --prune --tags --force

branch=apache-arrow-${version}
echo "Creating branch: ${branch}"
git checkout master
git branch -D ${branch} || :
git checkout -b ${branch}

echo "Updating apache-arrow formulae"
brew bump-formula-pr \
     --commit \
     --no-audit \
     --sha256="${sha256}" \
     --url="${url}" \
     --verbose \
     --write-only \
     apache-arrow

echo "Updating apache-arrow-glib formulae"
brew bump-formula-pr \
     --commit \
     --no-audit \
     --sha256="${sha256}" \
     --url="${url}" \
     --verbose \
     --write-only \
     apache-arrow-glib

for dependency in $(grep -l -r 'depends_on "apache-arrow"' Formula); do
  dependency=${dependency#Formula/}
  dependency=${dependency%.rb}
  if [ ${dependency} = "apache-arrow-glib" ]; then
    continue
  fi
  echo "Bumping revision of ${dependency} formulae"
  brew bump-revision --message "(apache-arrow ${version})" ${dependency}
done

# Force homebrew to not install from API but local checkout
export HOMEBREW_NO_INSTALL_FROM_API=1

echo "Testing apache-arrow formulae"
brew uninstall apache-arrow apache-arrow-glib || :
brew install --build-from-source apache-arrow
brew test apache-arrow
brew audit --strict apache-arrow

echo "Testing apache-arrow-glib formulae"
brew install --build-from-source apache-arrow-glib
brew test apache-arrow-glib
brew audit --strict apache-arrow-glib

git push -u $github_user ${branch}

git checkout -

popd

echo "Create a pull request:"
echo "  https://github.com/${github_user}/homebrew-core/pull/new/${branch}"
echo "with title: 'apache-arrow, apache-arrow-glib: ${version}'"
