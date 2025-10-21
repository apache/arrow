
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

# Packaging Checklist for CRAN Release

For a high-level overview of the Arrow release process see the [Apache Arrow Release Management Guide](https://arrow.apache.org/docs/developers/release.html#post-release-tasks).

## Before the Arrow Release Candidate Is Created

- [ ] [Create a GitHub issue](https://github.com/apache/arrow/issues/new/) entitled `[R] CRAN packaging checklist for version X.Y.Z` and copy this checklist to the issue.
- [ ] Review deprecated functions to advance their deprecation status.
- [ ] Evaluate the status of any failing [nightly tests and nightly packaging builds](http://crossbow.voltrondata.com). These checks replicate most of the checks that CRAN runs, so we need them all to be passing or to understand that the failures may (though won't necessarily) result in a rejection from CRAN.
- [ ] Check [current CRAN check results](https://cran.rstudio.org/web/checks/check_results_arrow.html).
- [ ] Ensure the contents of the README are accurate and up to date.
- [ ] Run `urlchecker::url_check()` on the R directory at the release candidate.
  commit. Ignore any errors with badges as they will be removed in the CRAN release branch.
- [ ] [Polish NEWS](https://style.tidyverse.org/news.html#news-release) but do **not** update version numbers (this is done automatically later). You can find commits by, for example, `git log --oneline <sha of last release>..HEAD | grep "\[R\]"`.
- [ ] For major releases, prepare content for social media highlighting new features.

_Wait for the release candidate to be created._

## After the Arrow Release Candidate Has Been Created

- [ ] Create a CRAN-release branch from the release candidate commit, name the new branch `maint-X.Y.Z-r` and push to upstream.

## Prepare and Check Package That Will Be Released to CRAN

- [ ] `git fetch upstream && git checkout maint-X.Y.Z-r && git clean -f -d`.
- [ ] Run `make build`. This copies Arrow C++ into tools/cpp, prunes some unnecessary components, and runs `R CMD build` to generate the source tarball. Because this will install the package, you will need to ensure that the version of Arrow C++ available to the configure script is the same as the version that is vendored into the R package (e.g., you may need to unset `ARROW_HOME`).
- [ ] `devtools::check_built("arrow_X.Y.Z.tar.gz")` locally.

## Wait for Arrow Release Vote

- [ ] Release vote passed

## Generate R Package to Submit to CRAN

- [ ] If the release candidate commit updated, rebase the CRAN release branch on that commit.
- [ ] Pick any commits that were made to main since the release commit that were needed to fix CRAN-related submission issues identified in the above steps.
- [ ] Remove badges from README.md.
- [ ] Run `urlchecker::url_check()` on the R directory.
- [ ] Create a PR entitled `WIP: [R] Verify CRAN release-X.Y.Z-rcX`. Add a comment `@github-actions crossbow submit --group r` to run all R crossbow jobs against the CRAN-specific release branch.
- [ ] Run `Rscript tools/update-checksums.R <libarrow version>` to download the checksums for the pre-compiled binaries from the ASF artifactory into the tools directory.
- [ ] Regenerate arrow_X.Y.Z.tar.gz (i.e., `make build`).

## Check Binary Arrow C++ Distributions Specific to the R Package

- [ ] Upload the .tar.gz to [win-builder](https://win-builder.r-project.org/upload.aspx) (r-devel only) and confirm with Jon (who will automatically receive an email about the results) that the check is clean.
- [ ] Upload the .tar.gz to [macOS Builder](https://mac.r-project.org/macbuilder/submit.html) and confirm that the check is clean.
- [ ] Check `install.packages("arrow_X.Y.Z.tar.gz")` on Ubuntu and ensure that the hosted binaries are used.
- [ ] `devtools::check_built("arrow_X.Y.Z.tar.gz")` locally one more time (for luck).

## Submit Package to CRAN

_This step must be done by the current package maintainer._

- [ ] Upload arrow_X.Y.Z.tar.gz to the [CRAN submit page](https://xmpalantir.wu.ac.at/cransubmit/).
- [ ] Confirm the submission email.

## Wait for CRAN to Accept the Submission

- [ ] CRAN has accepted the submission.
- [ ] Tag the tip of the CRAN-specific release branch with `r-universe-release`.
- [ ] Add a new line to the matrix in the [backwards compatability job](https://github.com/apache/arrow/blob/main/dev/tasks/r/github.linux.arrow.version.back.compat.yml).
- [ ] (patch releases only) Update the package version in `ci/scripts/PKGBUILD`, `r/DESCRIPTION`, and `r/NEWS.md`.
- [ ] (CRAN-only releases) Rebuild news page with `pkgdown::build_news()` and submit a PR to the asf-site branch of the docs site with the contents of `arrow/r/docs/news/index.html` replacing the current contents of `arrow-site/docs/r/news/index.html`.
- [ ] (CRAN-only releases) Bump the version number in `r/pkgdown/assets/versions.json`, and update this on the [the `asf-site` branch of the docs site](https://github.com/apache/arrow-site) too..
- [ ] Review the packaging checklist template and update as needed.
- [ ] Wait for CRAN-hosted binaries on the [CRAN package page](https://cran.r-project.org/package=arrow) to reflect the new version.
- [ ] Post already-prepared content to social media.
  - Use Bryce's [script](https://gist.githubusercontent.com/amoeba/4e26c064d1a0d0227cd8c2260cf0072a/raw/bc0d983152bdde4820de9074d4caee9986624bc5/new_contributors.R) for contributor calculation.
