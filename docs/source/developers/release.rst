.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. _release:

========================
Release Management Guide
========================

This page provides detailed information on the steps followed to perform
a release. It can be used both as a guide to learn the Apache Arrow release
process and as a comprehensive checklist for the Release Manager when
performing a release. The person acting as Release Manager must at least have
committer status in order to perform the tasks below. If the Release Manager is
a committer but not a member of the PMC, some tasks will need to be delegated
to a PMC member and these are marked below accordingly.

Principles
==========

The Apache Arrow Release follows the guidelines defined at the
`Apache Software Foundation Release Policy <https://www.apache.org/legal/release-policy.html>`_.

Preparing for the release
=========================

In advance to the Release date the Release Manager communicates with the community
usually via Zulip, Mailing List or the bi-weekly community call about the
upcoming release and proposes a feature freeze date.

The feature freeze date is the date the maintenance branch is created and
from that point no new features are allowed to be added to the release, unless there
is community consensus to allow it, and only bug fixes will be accepted.

Once the feature freeze is in place issues labelled as ``blocker`` must be resolved before
the first release candidate can be created.

Before creating a release candidate, the Release Manager must ensure that any
resolved GitHub issues have the appropriate milestone set so that the changelog
is generated properly.

Note that pull requests without a corresponding GitHub issue won't be detected
by the cherry-pick script and must be cherry-picked manually by the release
manager onto the maintenance branch. Examples include MINOR and Dependabot pull
requests. For this reason, it's encouraged to avoid the need for manual
cherry-picking by creating issues for any pull requests that are merged to the
default branch after the release maintenance branch has been created.

.. dropdown:: Requirements
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    Some steps of the release require being a committer or a PMC member.

    - Install the :ref:`Archery <archery>` utility which is required for the release.
    - You must not have any arrow-cpp or parquet-cpp environment variables defined except CC or CXX if you want to build with something other than GCC by default (e.g. clang).
    - A GPG key in the Apache Web of Trust to sign binary artifacts. This will have to be cross signed by other Apache committers/PMC members. If you have multiple GPG keys, you must set the correct GPG key ID in ``~/.gnupg/gpg.conf`` by adding:

    .. code-block::

        default-key ${YOUR_GPG_KEY_ID}

    - The GPG key needs to be added to this `SVN repo <https://dist.apache.org/repos/dist/dev/arrow/>`_ and `this one <https://dist.apache.org/repos/dist/release/arrow/>`_.
    - Have the build requirements for cpp and c_glib installed.
    - Install ``en_US.UTF-8`` locale. You can confirm available locales by ``locale -a``.
    - Install Python 3 as python
    - Create dev/release/.env from dev/release/.env.example. See the comments in dev/release/.env.example how to set each variable.
    - Setup :ref:`Crossbow<Crossbow>` as defined.
    - Have Docker and Docker Compose installed.
    - Have GitHub CLI installed.


Before creating a Release Candidate
===================================

Ensure local tags are removed, gpg-agent is set and GitHub issues are correctly
assigned.

.. code-block::

    # Delete the local tag for RC1 or later
    git tag -d apache-arrow-<version>

    # Setup gpg agent for signing binary artifacts
    source dev/release/setup-gpg-agent.sh

    # Curate the release
    # The end of the generated report shows any GitHub issues with the wrong
    # version number assigned.
    archery release curate <version>

Ensure a major version milestone for a follow up release is created on GitHub. This will
automatically be used by our merge script as the new version for issues closed when
the maintenance branch is created.

Patch Releases
==============

We usually create patch releases once a major breaking issue has been identified.
Issues that are identified as major breaking issues can be security fixes, broken packages
for specific builds and others.

Any developer can ask for a patch release to be generated sending an email to the
`Arrow development mailing-list <https://arrow.apache.org/community/>`__ with the reason
of why a new release is necessary.
If there is consensus and there is a Release Manager willing to make the effort to create
the release a patch release can be created.

Committers can tag issues that should be included on the next patch release using the
``backport-candidate`` label. Is the responsibility of the author or the committer to add the
label to the issue to help the Release Manager identify the issues that should be backported.

If a specific issue is identified as the reason to create a patch release the Release Manager
should validate that, at least, this issue is correctly tagged and included in the patch release.

Be sure to go through on the following checklist:

#. Create milestone
#. Create maintenance branch
#. Include issue that was requested as requiring new patch release
#. Add new milestone to issues with ``backport-candidate`` label
#. cherry-pick issues into maintenance branch

Creating a Release Candidate
============================

These are the different steps that are required to create a Release Candidate.

For the initial Release Candidate on a major release, we will create a maintenance
branch from main. This is done on the feature freeze date.

Follow up Release Candidates will update the maintenance branch by cherry-picking
specific commits.

For the initial Release Candidate for a minor or a patch release we will create
a maintenance branch from the previous corresponding release. For example,
for a 15.0.1 patch we will create a maint-15.0.1 branch from maint-15.0.0 and for
a maint-15.0.2 we will create it from maint-15.0.1. Once the maintenance branch is
created we will update the created maintenance branch by cherry-picking specific
commits.

Create or update the corresponding maintenance branch
-----------------------------------------------------

.. tab-set::

   .. tab-item:: Initial Release Candidate

      .. code-block::

            # Execute the following from an up to date main branch.
            # This will create a branch locally called maint-X.Y.Z.
            # X.Y.Z corresponds with the Major, Minor and Patch version number
            # of the release respectively. As an example 9.0.0
            archery release cherry-pick X.Y.Z --execute
            # Push the maintenance branch to the remote repository
            git push -u upstream maint-X.Y.Z

   .. tab-item:: Follow up Release Candidates

      .. code-block::

            # First run in dry-mode to see which commits will be cherry-picked.
            # If there are commits that we don't want to get applied, ensure the
            # milestone on GitHub is set to the following release.
            archery release cherry-pick X.Y.Z --continue
            # Update the maintenance branch with the previous commits
            archery release cherry-pick X.Y.Z --continue --execute
            # Push the updated maintenance branch to the remote repository
            git push -u upstream maint-X.Y.Z

Optional: Test Before Creating a Release Candidate
--------------------------------------------------

Some release managers prefer to perform testing before creating the first
release candidate to avoid the need to create multiple release candidates within
a given release.

To test before creating a release candidate:

* Create a pull request from the up-to-date maint-X.Y.Z branch onto main
* Title the pull request "WIP: Dummy PR to check maint-X.Y.Z status"
* Comment on the pull request to trigger the relevant Crossbow jobs:

  * ``@github-actions crossbow submit --group verify-rc-source``
  * ``@github-actions crossbow submit --group packaging``

Create the Release Candidate branch from the updated maintenance branch
-----------------------------------------------------------------------

.. code-block::

    # Start from the updated maintenance branch.
    git checkout maint-X.Y.Z

    # The following script will create a branch for the Release Candidate,
    # place the necessary commits updating the version number and then create a git tag
    # on OSX use gnu-sed with homebrew: brew install gnu-sed (and export to $PATH)
    #
    # <rc-number> starts at 0 and increments every time the Release Candidate is created
    # so for the first RC this would be: dev/release/01-prepare.sh 4.0.0 5.0.0 0
    dev/release/01-prepare.sh <version> <next-version> <rc-number>

    # Push the release candidate tag
    git push -u upstream apache-arrow-<version>-rc<rc-number>
    # Push the release candidate branch in order to trigger verification jobs later
    git push -u upstream release-<version>-rc<rc-number>


Once the tag is created the GitHub Actions workflow on ``verify-rc.yml`` will be triggered to verify
the release candidate.

The ``release_candidate.yml`` workflow will also be triggered which will sign the source code
for the release and will create a GitHub pre-release with the corresponding source and signatures.

Build source and binaries and submit them
-----------------------------------------

.. code-block::

    # Waits for previous workflows to finish and uploads source and signatures to SVN.
    #
    # NOTE: This must be run by a PMC member
    # NOTE: You need to have GitHub CLI installed to run this script.
    dev/release/02-source.sh <version> <rc-number>

    # Submit binary tasks using crossbow, the command will output the crossbow build id
    dev/release/03-binary-submit.sh <version> <rc-number>

    # Wait for the crossbow jobs to finish
    archery crossbow status <crossbow-build-id>

    # Download the produced binaries
    # This will download packages to a directory called packages/release-<version>-rc<rc-number>
    dev/release/04-binary-download.sh <version> <rc-number>

    # Sign and upload the binaries
    #
    # NOTE: This must be run by a PMC member
    #
    # On macOS the only way I could get this to work was running "echo "UPDATESTARTUPTTY" | gpg-connect-agent" before running this comment
    # otherwise I got errors referencing "ioctl" errors.
    dev/release/05-binary-upload.sh <version> <rc-number>

    # Sign and upload MATLAB artifacts to the GitHub Releases area.
    #
    # NOTE: This must be run by a PMC member
    # NOTE: You need to have GitHub CLI installed to run this script.
    dev/release/06-matlab-upload.sh <version> <rc-number>

    # Start verifications for binaries and wheels
    dev/release/07-binary-verify.sh <version> <rc-number>

Verify the Release
------------------

.. code-block::

    # Once the automatic verification has passed start the vote thread
    # on dev@arrow.apache.org. To regenerate the email template use
    SOURCE_DEFAULT=0 SOURCE_VOTE=1 dev/release/02-source.sh <version> <rc-number>

See :ref:`release_verification` for details.

Voting and approval
===================

Start the vote thread on dev@arrow.apache.org and supply instructions for verifying the integrity of the release.
Approval requires a net of 3 +1 votes from PMC members. A release cannot be vetoed.

Post-release tasks
==================

After the release vote, we must undertake many tasks to update source artifacts, binary builds, and the Arrow website.

Be sure to go through on the following checklist:

#. Update the released milestone Date and set to "Closed" on GitHub
#. Merge changes on release branch to maintenance branch for patch releases
#. Add the new release to the Apache Reporter System
#. Push release tag
#. Upload source
#. Upload binaries
#. Update website
#. Update GitHub Release Notes
#. Update Homebrew packages
#. Update MSYS2 package
#. Upload RubyGems
#. Update conda recipes
#. Upload wheels/sdist to pypi
#. Update R packages
#. Update vcpkg port
#. Update Conan recipe
#. Bump versions
#. Update docs
#. Update version in Apache Arrow Cookbook
#. Announce the new release
#. Publish release blog posts
#. Announce the release on BlueSky
#. Remove old artifacts

.. dropdown:: Merge changes on release branch to maintenance branch for patch releases
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Merge ``release-X.Y.Z-rcN`` to ``maint-X.Y.Z``:

   .. code-block:: Bash

      # git checkout maint-10.0.0
      git checkout maint-X.Y.Z
      # git merge release-10.0.0-rc0
      git merge release-X.Y.Z-rcN
      # git push -u upstream maint-10.0.0
      git push -u upstream maint-X.Y.Z

.. dropdown:: Add the new release to the Apache Reporter System
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Add relevant release data for Arrow to `Apache reporter <https://reporter.apache.org/addrelease.html?arrow>`_.

.. dropdown:: Push release tag and create GitHub Release
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   A committer must push the release tag to GitHub:

   .. code-block:: Bash

      # dev/release/post-01-tag.sh 0.1.0 0
      dev/release/post-01-tag.sh <version> <rc>

.. dropdown:: Upload source release artifacts to Subversion
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   A PMC member must commit the source release artifacts to Subversion:

   .. code-block:: Bash

      # dev/release/post-02-upload.sh 0.1.0 0
      dev/release/post-02-upload.sh <version> <rc>

.. dropdown:: Upload binary release artifacts to Artifactory
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   A committer must upload the binary release artifacts to Artifactory and create the GitHub Release:

   .. code-block:: Bash

      # dev/release/post-03-binary.sh 0.1.0 0
      dev/release/post-03-binary.sh <version> <rc number>

.. dropdown:: Update website
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Add a release note for the new version to our website and update the latest release information:

   .. code-block:: Bash

      ## Prepare your fork of https://github.com/apache/arrow-site .
      ## You need to do this only once.
      # git clone git@github.com:kou/arrow-site.git ../
      git clone git@github.com:<YOUR_GITHUB_ID>/arrow-site.git ../
      cd ../arrow-site
      ## Add git@github.com:apache/arrow-site.git as "upstream" remote.
      git remote add upstream git@github.com:apache/arrow-site.git
      cd -

      ## Generate a release note for the new version, update the
      ## latest release information automatically.
      # dev/release/post-04-website.sh 9.0.0 10.0.0
      dev/release/post-04-website.sh OLD_X.OLD_Y.OLD_Z X.Y.Z

   This script pushes a ``release-note-X.Y.Z`` branch to your ``apache/arrow-site`` fork. You need to open a pull request from the ``release-note-X.Y.Z`` branch on your Web browser.

.. dropdown:: Update Release Notes in apache/arrow GitHub Release
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   A committer must run the following script. This has to be done once the
   Pull Request from the Update Website script has been merged:

   .. code-block:: Bash

      # dev/release/post-05-update-gh-release-notes.sh 17.0.0
      dev/release/post-05-update-gh-release-notes.sh <version>

.. dropdown:: Update Homebrew packages
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Open a pull request to Homebrew:

   .. code-block:: Bash

      ## You need to run this on macOS or Linux that Homebrew is installed.

      ## Fork https://github.com/Homebrew/homebrew-core on GitHub.
      ## You need to do this only once.
      ##
      ## Prepare your fork of https://github.com/Homebrew/homebrew-core .
      ## You need to do this only once.
      cd "$(brew --repository homebrew/core)"
      # git remote add kou git@github.com:kou/homebrew-core.git
      git remote add <YOUR_GITHUB_ID> git@github.com:<YOUR_GITHUB_ID>/homebrew-core.git
      cd -

      # dev/release/post-12-homebrew.sh 10.0.0 kou
      dev/release/post-12-homebrew.sh X.Y.Z <YOUR_GITHUB_ID>

   This script pushes a ``apache-arrow-X.Y.Z`` branch to your ``Homebrew/homebrew-core`` fork. You need to create a pull request from the ``apache-arrow-X.Y.Z`` branch with ``apache-arrow, apache-arrow-glib: X.Y.Z`` title on your Web browser.

.. dropdown:: Update MSYS2 packages
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Open a pull request to MSYS2:

   .. code-block:: Bash

      ## Fork https://github.com/msys2/MINGW-packages on GitHub.
      ## You need to do this only once.
      ##
      ## Prepare your fork of https://github.com/msys2/MINGW-packages .
      ## You need to do this only once.
      # git clone git@github.com:kou/MINGW-packages.git ../
      git clone git@github.com:<YOUR_GITHUB_ID>/MINGW-packages.git ../
      cd ../MINGW-packages
      ## Add https://github.com/msys2/MINGW-packages.git as "upstream" remote.
      git remote add upstream https://github.com/msys2/MINGW-packages.git
      cd -

      # dev/release/post-11-msys2.sh 10.0.0 ../MINGW-packages
      dev/release/post-11-msys2.sh X.Y.Z <YOUR_MINGW_PACKAGES_FORK>

   This script pushes a ``arrow-X.Y.Z`` branch to your ``msys2/MINGW-packages`` fork. You need to create a pull request from the ``arrow-X.Y.Z`` branch with ``arrow: Update to X.Y.Z`` title on your Web browser.

.. dropdown:: Update RubyGems
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   You need an account on https://rubygems.org/ to release Ruby packages.

   If you have an account on https://rubygems.org/ , you need to join owners of our gems.

   Existing owners can add a new account to the owners of them by the following command line:

   .. code-block:: Bash

      # dev/release/account-ruby.sh raulcd
      dev/release/account-ruby.sh NEW_ACCOUNT

   Update RubyGems after Homebrew packages and MSYS2 packages are updated:

   .. code-block:: Bash

      # dev/release/post-06-ruby.sh 10.0.0
      dev/release/post-06-ruby.sh X.Y.Z

.. dropdown:: Upload wheels/sdist to PyPI
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   pip binary packages (called "wheels") and source package (called "sdist") are built using the crossbow tool that we used above during the release candidate creation process and then uploaded to PyPI (Python Package Index) under the pyarrow package.

   We use the twine tool to upload wheels to PyPI:

   .. code-block:: Bash

      # dev/release/post-09-python.sh 10.0.0
      dev/release/post-09-python.sh <version>

.. dropdown:: Update R packages
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   To publish the R package on CRAN, there are a few steps we need to do first
   in order to ensure that binaries for Windows and macOS are available to CRAN.
   Jeroen Ooms <jeroenooms@gmail.com> maintains several projects that build C++
   dependencies for R packages for macOS and Windows. We test copies of these
   same build scripts in our CI, and at release time, we need to send any
   changes we have and update the versions/hashes upstream.

   When the release candidate is made, make draft pull requests to each
   repository using the rc, updating the version and SHA, as well as any cmake
   build changes from the corresponding files in apache/arrow. Jeroen may
   merge these PRs before the release vote passes, build the binary artifacts,
   and publish them in the right places so that we can do pre-submission checks
   (see below). After the release candidate vote passes, update these PRs
   to point to the official (non-rc) URL and mark them as ready for review.
   Jeroen will merge, build the binary artifacts, and publish them in the
   right places. See the
   `packaging checklist <https://github.com/apache/arrow/blob/main/r/PACKAGING.md>`_.
   for a precise list of pull requests that must be made prior to submission
   to CRAN.

   Once these binary prerequisites have been satisfied, we can submit to CRAN.
   Given the vagaries of the process, it is best if the R developers on the
   project verify the CRAN-worthiness of the package before submitting.
   Our CI systems give us some coverage for the things that CRAN checks, but
   there are a couple of final tests we should do to confirm that the release
   binaries will work and that everything runs on the same infrastructure that
   CRAN has, which is difficult/impossible to emulate fully with Docker. For a
   precise list of checks, see the
   `packaging checklist <https://github.com/apache/arrow/blob/main/r/PACKAGING.md>`_.

   Once all checks are clean, we submit to CRAN, which has a web form for
   uploading packages. The release process requires email confirmation
   from the R package maintainer, currently Neal Richardson.

.. dropdown:: Update vcpkg port
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Open a pull request to vcpkg:

   .. code-block:: Bash

      ## Fork https://github.com/microsoft/vcpkg on GitHub.
      ## You need to do this only once.
      ##
      ## Prepare your fork of https://github.com/microsoft/vcpkg .
      ## You need to do this only once.
      # git clone git@github.com:kou/vcpkg.git ../
      git clone git@github.com:<YOUR_GITHUB_ID>/vcpkg.git ../
      cd ../vcpkg
      ./bootstrap-vcpkg.sh
      ## Add https://github.com/microsoft/vcpkg.git as "upstream" remote.
      git remote add upstream https://github.com/microsoft/vcpkg.git
      cd -

      # dev/release/post-13-vcpkg.sh 10.0.0 ../vcpkg
      dev/release/post-13-vcpkg.sh X.Y.Z <YOUR_VCPKG_FORK>

   This script pushes a ``arrow-X.Y.Z`` branch to your ``microsoft/vcpkg`` fork. You need to create a pull request from the ``arrow-X.Y.Z`` branch with ``[arrow] Update to X.Y.Z`` title on your Web browser.

.. dropdown:: Update Conan port
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Open a pull request to Conan:

   .. code-block:: Bash

      ## Fork https://github.com/conan-io/conan-center-index on GitHub.
      ## You need to do this only once.
      ##
      ## Prepare your fork of https://github.com/conan-io/conan-center-index .
      ## You need to do this only once.
      # git clone git@github.com:kou/conan-center-index.git ../
      git clone git@github.com:<YOUR_GITHUB_ID>/conan-center-index.git ../
      cd ../conan-center-index
      ## Add https://github.com/conan-io/conan-center-index.git as "upstream" remote.
      git remote add upstream https://github.com/conan-io/conan-center-index.git
      cd -

      # dev/release/post-14-conan.sh 10.0.1 ../conan-center-index
      dev/release/post-14-conan.sh X.Y.Z <YOUR_CONAN_CENTER_INDEX_FORK>

   This script pushes a ``arrow-X.Y.Z`` branch to your ``conan-io/conan-center-index`` fork. You need to create a pull request from the ``arrow-X.Y.Z`` branch on your Web browser.

.. dropdown:: Bump versions
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   .. code-block:: Bash

      # You can run the script with BUMP_TAG=0 and BUMP_PUSH=0
      # this will avoid default pushing to main and pushing the tag
      # but you will require to push manually after reviewing the commits.
      # dev/release/post-10-bump-versions.sh 10.0.0 11.0.0
      dev/release/post-10-bump-versions.sh X.Y.Z NEXT_X.NEXT_Y.NEXT_Z

.. dropdown:: Update docs
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Documentation is generated as part of the release process. We just need to
   upload the generated documentation:

   .. code-block:: Bash

      ## Prepare your fork of https://github.com/apache/arrow-site .
      ## You need to do this only once.
      # git clone git@github.com:kou/arrow-site.git ../
      git clone git@github.com:<YOUR_GITHUB_ID>/arrow-site.git ../
      cd ../arrow-site
      ## Add git@github.com:apache/arrow-site.git as "upstream" remote.
      git remote add upstream git@github.com:apache/arrow-site.git
      cd -

      # dev/release/post-08-docs.sh 10.0.0 9.0.0
      dev/release/post-08-docs.sh X.Y.Z PREVIOUS_X.PREVIOUS_Y.PREVIOUS_Z

   This script pushes a ``release-docs-X.Y.Z`` branch to your ``arrow-site`` fork. You need to create a Pull Request and use the ``asf-site`` branch as base for it.

.. dropdown:: Update version in Apache Arrow Cookbook
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Follow `the documentation <https://github.com/apache/arrow-cookbook/tree/main/dev/release>`_
   in the Apache Arrow Cookbook repository

.. dropdown:: Announce the new release
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Write a release announcement (see `example <https://lists.apache.org/thread/6rkjwvyjjfodrxffllh66pcqnp729n3k>`_) and send to announce@apache.org and dev@arrow.apache.org.

   The announcement to announce@apache.org must be sent from your apache.org e-mail address to be accepted.

.. dropdown:: Publish release blog post
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   The blog post process isn't automated. The rough set of steps we usually take
   are:

   * Clone https://github.com/apache/arrow-site.
   * Create a new branch off ``main`` for the blog post pull request we're
     creating.
   * Duplicate a recent blog post entry in the ``_posts`` subfolder and update
     the filename and YAML metadata.

     * Set the date in the filename and in the YAML metadata to the date that the
       release candidate vote thread for the release closed (in GMT).

   * *For minor releases only*, remove any section about community updates (new
     committers, PMC members, etc).
   * Update the remainder of the text as needed
   * Create the pull request
   * In the pull request, ping contributors in each section requesting help
     filling in the details for each section.


.. dropdown:: Announce the release on social media
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Post about the release and link to the blog post on social media. The project
   has two official accounts:

   * BlueSky: `@arrow.apache.org <https://bsky.app/profile/arrow.apache.org>`_
   * LinkedIn: https://www.linkedin.com/company/apache-arrow/

   PMC members have access or can request access to post under these accounts.

.. dropdown:: Remove old artifacts
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Remove RC artifacts on https://dist.apache.org/repos/dist/dev/arrow/ and old release artifacts on https://dist.apache.org/repos/dist/release/arrow to follow `the ASF policy <https://infra.apache.org/release-download-pages.html#current-and-older-releases>`_:

   .. code-block:: Bash

      dev/release/post-07-remove-old-artifacts.sh

   Note: This step must be done by a PMC member.
