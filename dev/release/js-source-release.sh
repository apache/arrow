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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <js-version> <rc-num>"
  exit
fi

js_version=$1
rc=$2

tag=apache-arrow-js-${js_version}
tagrc=${tag}-rc${rc}

echo "Preparing source for tag ${tag}"

tarball=${tag}.tar.gz

# cd to $ARROW_HOME/js
cd $SOURCE_DIR/../../js
JS_SRC_DIR="$PWD"
# npm pack the js source files
npm install

npm version --no-git-tag-version $js_version
git add package.json
git commit -m "[Release] Apache Arrow JavaScript $js_version"
git tag -a ${tag}

release_hash=`git rev-list $tag 2> /dev/null | head -n 1 `

if [ -z "$release_hash" ]; then
  echo "Cannot continue: unknown git tag: $tag"
  exit
fi

echo "Using commit $release_hash"

cd $SOURCE_DIR

rm -rf js-tmp
# `npm pack` writes the .tgz file to the current dir, so cd into js-tmp
mkdir -p js-tmp
cd js-tmp
# run npm pack on `arrow/js`
npm pack ${JS_SRC_DIR}
# unzip and remove the npm pack tarball
tar -xzf *.tgz && rm *.tgz
# `npm pack` puts files in a dir called "package"
cp $JS_SRC_DIR/../NOTICE.txt package
cp $JS_SRC_DIR/../LICENSE.txt package
# rename "package" to $tag
mv package ${tag}
tar czf ${tarball} ${tag}
rm -rf ${tag}

${SOURCE_DIR}/run-rat.sh ${tarball}

# sign the archive
gpg --armor --output ${tarball}.asc --detach-sig ${tarball}
sha1sum $tarball > ${tarball}.sha1
sha256sum $tarball > ${tarball}.sha256
sha512sum $tarball > ${tarball}.sha512

# check out the arrow RC folder
svn co --depth=empty https://dist.apache.org/repos/dist/dev/arrow js-rc-tmp

# add the release candidate for the tag
mkdir -p js-rc-tmp/${tagrc}
cp ${tarball}* js-rc-tmp/${tagrc}
svn add js-rc-tmp/${tagrc}
svn ci -m 'Apache Arrow JavaScript ${version} RC${rc}' js-rc-tmp/${tagrc}

cd -

# clean up
rm -rf js-tmp

echo "Success! The release candidate is available here:"
echo "  https://dist.apache.org/repos/dist/dev/arrow/${tagrc}"
echo ""
echo "Commit SHA1: ${release_hash}"
