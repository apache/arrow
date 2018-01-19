<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Guide for Arrow Release Managers

This document is intended to provide a comprehensive checklist of tasks before,
during, and after an Arrow release.

## Preparing for the release

### JIRA tidying

Before creating a source release, the release manager must ensure that any
resolved JIRAs have the appropriate Fix Version set so that the changelog is
generated properly.

To do this, search for the Arrow project and issues with no fix version. Click
the "Tools" dropdown menu in the top right of the page and select "Bulk
Change". Indicate that you wish to edit the issues, then set the correct Fix
Version and apply the change. Remember to uncheck the box about "send e-mail
notifications" to avoid excess spam to issues@arrow.apache.org.

## Main source release and vote

### Source release and vote

Follow the instructions in [dev/release/README][1] to produce the source
release artifacts. PMC karma is required to upload these files to the dist
system; they are uploaded automatically by the source release script.

Start the vote thread on dev@arrow.apache.org and supply intructions for
verifying the integrity of the release. Approval requires a net of 3 +1 votes
from PMC members. A release cannot be vetoed.

## Post-release tasks

After the release vote, we must undertake many tasks to update source
artifacts, binary builds, and the Arrow website.

### Updating the Arrow website

The website is a Jekyll project in the `site/` directory in the main Arrow
repository. As part of updating the website, we must perform various subtasks.

First, create a new entry to add to http://arrow.apache.org/release/; these are
in the `_release` subdirectory. The new contents of the new entry will go into
a new Markdown file of the form `X.Y.Z.md`. You can start by copying one of the
other release entries.

Generate a web-friendly changelog by running (python3)

```
dev/release/changelog.py $VERSION 1
```

Copy and paste the result.

Update `index.html` as appropriate for the new release. Then update
`install.md` to include links for the new release.

Finally, if appropriate, write a short blog post summarizing the new release
highlights. [Here is an example.][8]

### Uploading release artifacts to SVN

A PMC must commit the source release artifacts to SVN at:

```
https://dist.apache.org/repos/dist/release/arrow
```

Create a new directory in SVN of the form `arrow-X.Y.Z`. If possible, remove
any old releases to reduce load on the ASF mirror system.

### Announcing release

Write a release announcement ([see example][9]) and send to announce@apache.org
and dev@arrow.apache.org. The announcement to announce@apache.org must be send
from your apache.org e-mail address to be accepted.

### Updating website with new API documentation

The API documentation for `C++`, `C Glib`, `Python` and `Java` can be generated
via a Docker-based setup. To generate the API documentation run the following
command:

```shell
bash dev/gen_apidocs.sh
```

This script assumes that the `parquet-cpp` Git repository
https://github.com/apache/parquet-cpp has been cloned
besides the Arrow repository and a `dist` directory can be created
at the same level by the current user. Please note that most of the
software must be built in order to create the documentation, so this
step may take some time to run, especially the first time around as the
Docker container will also have to be built.

To upload the updated documentation to the website, navigate to `site/asf-site`
and commit all changes:

```
pushd site/asf-site
git add .
git commit -m "Updated API documentation for version X.Y.Z"
```

After successfully creating the API documentation the website can be
run locally to browse the API documentation from the top level
`Documentation` menu. To run the website issue the command:

```shell
bash dev/run_site.sh
```

The local URL for the website running inside the docker container
will be shown as `Server address:` in the output of the command.
To stop the server press `Ctrl-C` in that window.

### Updating C++ and Python packages

We have been making Arrow available to C++ and Python users on the 3 major
platforms (Linux, macOS, and Windows) via two package managers: pip and conda.

#### Updating pip packages

The pip binary packages (called "wheels") are generated from the
[arrow-dist][2] repository. This is a multi-step process:

* Unfortunately, we are unable to upload to the Apache Arrow BinTray account,
  so if you are the release manager, make sure to enable arrow-dist on your
  Appveyor account and create a BinTray project under your personal BinTray
  account where the artifacts can be written.
* Update `.travis.yml` to reference the new Arrow release tag
* Update `appveyor.yml` to reference the correct Arrow git commit and
  corresponding version of [parquet-cpp][3]. At the end of this file there are
  instructions for deploying the Windows packages to BinTray.
* Push arrow-dist updates to **both** apache/arrow-dist and your fork of
  arrow-dist.
* Wait for builds to complete
* Download all wheel files from the new BinTray package version ([example][4])

Now, you can finally upload the wheels to PyPI using the `twine` CLI tool. You
must be permissioned on PyPI to upload here; ask Wes McKinney or Uwe Korn if
you need help with this.

#### Updating conda packages

We have been building conda packages using [conda-forge][6]. The three
"feedstocks" that must be updated in-order are:

* https://github.com/conda-forge/arrow-cpp-feedstock
* https://github.com/conda-forge/parquet-cpp-feedstock
* https://github.com/conda-forge/pyarrow-feedstock

To update a feedstock, open a pull request updating `meta.yaml` as
appropriate. Once you are confident that the build is good and the metadata is
updated properly, merge the pull request. You must wait until the results of
each of the feedstocks land in [anaconda.org][7] before moving on to the next
package.

Unfortunately, you cannot open pull requests to all three repositories at the
same time because they are interdependent.

### Updating Java Maven artifacts in Maven central

See instructions at end of https://github.com/apache/arrow/blob/master/dev/release/README

[1]: https://github.com/apache/arrow/blob/master/dev/release/README
[2]: https://github.com/apache/arrow-dist
[3]: https://github.com/apache/parquet-cpp
[4]: https://bintray.com/wesm/apache-arrow-test/pyarrow/0.7.1#files
[5]: https://pypi.python.org/pypi/pyarrow
[6]: https://conda-forge.org/
[7]: https://anaconda.org
[8]: http://arrow.apache.org/blog/2017/09/19/0.7.0-release/
[9]: http://mail-archives.apache.org/mod_mbox/www-announce/201709.mbox/%3CCAJPUwMC+VDRQ+Qj25_pqoq+bvs0bsk2Vx614OUpYwTHteFOVGw@mail.gmail.com%3E
