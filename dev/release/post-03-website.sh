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

set -e
set -u

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARROW_DIR="${SOURCE_DIR}/../.."
ARROW_SITE_DIR="${ARROW_DIR}/../arrow-site"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <previous-version> <version>"
  exit 1
fi

previous_version=$1
version=$2

branch_name=release-note-${version}
release_dir="${ARROW_SITE_DIR}/_release"
announce_file="${release_dir}/${version}.md"
versions_yml="${ARROW_SITE_DIR}/_data/versions.yml"

pushd "${ARROW_SITE_DIR}"
git checkout master
git checkout -b ${branch_name}
popd

pushd "${ARROW_DIR}"

release_date=$(LANG=C date "+%-d %B %Y")
previous_tag_date=$(git log -n 1 --pretty=%aI apache-arrow-${previous_version})
rough_previous_release_date=$(date --date "${previous_tag_date}" +%s)
rough_release_date=$(date +%s)
rough_n_development_months=$((
  (${rough_release_date} - ${rough_previous_release_date}) / (60 * 60 * 24 * 30)
))

git_tag=apache-arrow-${version}
git_range=apache-arrow-${previous_version}..${git_tag}

committers_command_line="git shortlog -csn ${git_range}"
contributors_command_line="git shortlog -sn ${git_range}"

committers=$(${committers_command_line})
contributors=$(${contributors_command_line})

n_commits=$(git log --pretty=oneline ${git_range} | wc -l)
n_contributors=$(${contributors_command_line} | wc -l)

git_tag_hash=$(git log -n 1 --pretty=%H ${git_tag})

popd

pushd "${ARROW_SITE_DIR}"

# Add announce for the current version
cat <<ANNOUNCE > "${announce_file}"
---
layout: default
title: Apache Arrow ${version} Release
permalink: /release/${version}.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Apache Arrow ${version} (${release_date})

This is a major release covering more than ${rough_n_development_months} months of development.

## Download

* [**Source Artifacts**][1]
* **Binary Artifacts**
  * [For CentOS][2]
  * [For Debian][3]
  * [For Python][4]
  * [For Ubuntu][5]
* [Git tag][6]

## Contributors

This release includes ${n_commits} commits from ${n_contributors} distinct contributors.

\`\`\`console
$ ${contributors_command_line}
ANNOUNCE

echo "${contributors}" >> "${announce_file}"

cat <<ANNOUNCE >> "${announce_file}"
\`\`\`

## Patch Committers

The following Apache committers merged contributed patches to the repository.

\`\`\`console
$ ${committers_command_line}
ANNOUNCE

echo "${committers}" >> "${announce_file}"

cat <<ANNOUNCE >> "${announce_file}"
\`\`\`

## Changelog

ANNOUNCE

# TODO(kszucs): needs to update for the new changelog generation with archery
${PYTHON:-python} "${SOURCE_DIR}/changelog.py" ${version} 1 | \
  sed -e 's/^#/##/g' >> "${announce_file}"

cat <<ANNOUNCE >> "${announce_file}"
[1]: https://www.apache.org/dyn/closer.lua/arrow/arrow-${version}/
[2]: https://bintray.com/apache/arrow/centos/${version}/
[3]: https://bintray.com/apache/arrow/debian/${version}/
[4]: https://bintray.com/apache/arrow/python/${version}/
[5]: https://bintray.com/apache/arrow/ubuntu/${version}/
[6]: https://github.com/apache/arrow/releases/tag/apache-arrow-${version}
ANNOUNCE
git add "${announce_file}"


# Update index
pushd "${release_dir}"

index_file=index.md
rm -f ${index_file}
announce_files="$(ls | sort --version-sort --reverse)"
cat <<INDEX > ${index_file}
---
layout: default
title: Releases
permalink: /release/index.html
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# Apache Arrow Releases

Navigate to the release page for downloads and the changelog.

INDEX

i=0
for md_file in ${announce_files}; do
  i=$((i + 1))
  title=$(grep '^# Apache Arrow' ${md_file} | sed -e 's/^# Apache Arrow //')
  echo "* [${title}][${i}]" >> ${index_file}
done
echo >> ${index_file}

i=0
for md_file in ${announce_files}; do
  i=$((i + 1))
  html_file=$(echo ${md_file} | sed -e 's/md$/html/')
  echo "[${i}]: {{ site.baseurl }}/release/${html_file}" >> ${index_file}
done

git add ${index_file}

popd


# Update versions.yml
pinned_version=$(echo ${version} | sed -e 's/\.[^.]*$/.*/')

apache_download_url=https://downloads.apache.org

cat <<YAML > "${versions_yml}"
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Database of the current version
#
current:
  number: '${version}'
  pinned_number: '${pinned_version}'
  date: '${release_date}'
  git-tag: '${git_tag_hash}'
  github-tag-link: 'https://github.com/apache/arrow/releases/tag/${git_tag}'
  release-notes: 'https://arrow.apache.org/release/${version}.html'
  mirrors: 'https://www.apache.org/dyn/closer.lua/arrow/arrow-${version}/'
  tarball_name: 'apache-arrow-${version}.tar.gz'
  mirrors-tar: 'https://www.apache.org/dyn/closer.lua/arrow/arrow-${version}/apache-arrow-${version}.tar.gz'
  java-artifacts: 'http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.arrow%22%20AND%20v%3A%22${version}%22'
  asc: '${apache_download_url}/arrow/arrow-${version}/apache-arrow-${version}.tar.gz.asc'
  sha256: '${apache_download_url}/arrow/arrow-${version}/apache-arrow-${version}.tar.gz.sha256'
  sha512: '${apache_download_url}/arrow/arrow-${version}/apache-arrow-${version}.tar.gz.sha512'
YAML
git add "${versions_yml}"

git commit -m "[Website] Add release note for ${version}"
git push -u origin ${branch_name}

github_url=$(git remote get-url origin | \
               sed \
                 -e 's,^git@github.com:,https://github.com/,' \
                 -e 's,\.git$,,')

echo "Success!"
echo "Create a pull request:"
echo "  ${github_url}/pull/new/${branch_name}"

popd
