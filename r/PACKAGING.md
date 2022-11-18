
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

# Packaging checklist for CRAN release

For full documentation of the steps in this checklist, see the
[Apache Arrow Release Management Guide](https://cwiki.apache.org/confluence/display/ARROW/Release+Management+Guide#ReleaseManagementGuide-UpdatingRpackages).

- [ ] When a release candidate is created
  [Create a GitHub issue](https://github.com/apache/arrow/issues/new/)
  entitled `[R] CRAN release version X.X.X` and copy this checklist to the issue
  
- [ ] Confirm that all
  [nightly tests and nightly packaging builds](https://lists.apache.org/list.html?builds@arrow.apache.org) are passing.
- [ ] Check [current CRAN check results](https://cran.rstudio.org/web/checks/check_results_arrow.html)
- [ ] Check README
- [ ] `urlchecker::url_check()`
- [ ] [Polish NEWS](https://style.tidyverse.org/news.html#news-release)
- [ ] Prepare tweet thread highlighting new features

Make draft pull requests into the [autobrew](https://github.com/autobrew) and
[rtools-packages](https://github.com/r-windows/rtools-packages) repositories
used by the configure script on MacOS and Windows:

- [ ] Open a draft pull request to modify 
  [the apache-arrow autobrew formula]( https://github.com/autobrew/homebrew-core/blob/master/Formula/apache-arrow.rb) 
  to update the version, SHA, and any changes to dependencies and build steps that have changed in the
  [copy of the formula we have of that formula in the Arrow repo](https://github.com/apache/arrow/blob/master/dev/tasks/homebrew-formulae/autobrew/apache-arrow.rb)
- [ ] Open a draft pull request to modify 
  [the apache-arrow-static autobrew formula]( https://github.com/autobrew/homebrew-core/blob/master/Formula/apache-arrow-static.rb) 
  to update the version, SHA, and any changes to dependencies and build steps that have changed in the
  [copy of the formula we have of that formula in the Arrow repo](https://github.com/apache/arrow/blob/master/dev/tasks/homebrew-formulae/autobrew/apache-arrow-static.rb)
- [ ] Open a draft pull request to modify the 
  [autobrew script](https://github.com/autobrew/scripts/blob/master/apache-arrow)
  to include any additions made to
  [r/tools/autobrew](https://github.com/apache/arrow/blob/master/r/tools/autobrew).
- [ ] Open a draft pull request to modify the
  [PKGBUILD script](https://github.com/r-windows/rtools-packages/blob/master/mingw-w64-arrow/PKGBUILD)
  to reflect changes in
  [ci/PKGBUILD](https://github.com/apache/arrow/blob/master/ci/scripts/PKGBUILD),
  uncommenting the line that says "uncomment to test the rc".

Prepare and check the .tar.gz that will be released to CRAN:

TODO: The packaging guide says `make release` but isn't there also a step where
we bundle Arrow C++ into tools/cpp and prune it so that the .tar.gz stays under 5 MB?

- `git fetch upstream && git checkout release-X.X.X-rc0`
- [ ] `devtools::check("arrow_X.X.X.tar.gz")` locally on Linux
- [ ] `devtools::check("arrow_X.X.X.tar.gz")` locally on Windows
- [ ] `devtools::check("arrow_X.X.X.tar.gz")` locally on MacOS
- [ ] `devtools::check("arrow_X.X.X.tar.gz")` locally on MacOS M1
- [ ] `devtools::check("arrow_X.X.X.tar.gz")` locally on Linux
- [ ] `rhub::check("arrow_X.X.X.tar.gz", platform=c("debian-gcc-patched", "fedora-clang-devel"))`
  (TODO: as I understand it, we can't do rhub Windows and Mac checks because those
  would require the PRs into autobrew/rtools-packages to be finalized? Or maybe
  these already know about/use the nightly binaries?)
- [ ] Run reverse dependency checks locally (TODO: document how to do this
  since we can't just use `revdepcheck`)

After this step, we need an official release:
  
- [ ] Wait for the release vote to pass

TODO is this where we create a CRAN release branch where the minor CRAN-specific
fixes get picked from master? What is the git incantation for that?

- [ ] Bump the Version in DESCRIPTION to X.X.X
- [ ] Regenerate arrow_X.X.X.tar.tz (via `make release`?)

Finalize and merge draft PRs/ensure linux binary packages are available:

- [ ] PR into autobrew/homebrew-core
- [ ] PR into autobrew/homebrew-core
- [ ] PR into autobrew/scripts
- [ ] PR into r-windows/rtools-packages
- [ ] Ensure linux binaries are available (TODO: maybe document where these live?
  I don't remember where this is at the top of my head)

Check binary Arrow C++ distributions specific to the R package:

- [ ] Upload the .tar.gz to [WinBuilder](https://win-builder.r-project.org/upload.aspx)
  and confirm that the check is clean
- [ ] Upload the .tar.gz to [MacBuilder](https://mac.r-project.org/macbuilder/submit.html)
  and confirm that the check is clean
- [ ] Check `install.packages("arrow_X.X.X.tar.gz")` on Ubuntu and ensure that the
  hosted binaries are used
  
Submit!

- [ ] Upload arrow_X.X.X.tar.gz to the
  [CRAN submit page](https://xmpalantir.wu.ac.at/cransubmit/)
- [ ] Confirm the submission email

Wait for CRAN...

Post release:

- [ ] Accepted!
- [ ] Tag the tip of the CRAN-specific release branch?
- [ ] Bump the version number in DESCRIPTION to XXX?
- [ ] Add a new line to the matrix in the [backwards compatability job](https://github.com/apache/arrow/blob/master/dev/tasks/r/github.linux.arrow.version.back.compat.yml)
- [ ] Update the packaging checklist template to reflect any new realities of the
  packaging process.
- [ ] Wait for CRAN-hosted binaries on the
  [CRAN package page](https://cran.r-project.org/package=arrow) to reflect the
  new version
