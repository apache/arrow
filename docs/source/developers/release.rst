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

========================
Release Management Guide
========================

This page provides detailed information on the steps followed to perform
a release. It can be used both as a guide to learn the Apache Arrow release
process and as a comprehensive checklist for the Release Manager when
performing a release.

Principles
==========

The Apache Arrow Release follows the guidelines defined at the
`Apache Software Foundation Release Policy <https://www.apache.org/legal/release-policy.html>`_.

Preparing for the release
=========================

Before creating a source release, the Release Manager must ensure that any
resolved JIRAs have the appropriate Fix Version set so that the changelog is
generated properly.

.. dropdown:: Requirements
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    Some steps of the release require being a committer or a PMC member.

    - Install the :ref:`Archery <archery>` utility which is required for the release.
    - You must not have any arrow-cpp or parquet-cpp environment variables defined except CC or CXX if you want to build with something other than GCC by default (e.g. clang).
    - A GPG key in the Apache Web of Trust to sign artifacts. This will have to be cross signed by other Apache committers/PMC members. If you have multiple GPG keys, you must set the correct GPG key ID in ``~/.gnupg/gpg.conf`` by adding:

    .. code-block::

        default-key ${YOUR_GPG_KEY_ID}

    - The GPG key needs to be added to this `SVN repo <https://dist.apache.org/repos/dist/dev/arrow/>`_ and `this one <https://dist.apache.org/repos/dist/release/arrow/>`_.
    - Configure Maven to `publish artifacts to Apache repositories <http://www.apache.org/dev/publishing-maven-artifacts.html>`_. You will need to `setup a master password <https://maven.apache.org/guides/mini/guide-encryption.html>`_ at ``~/.m2/settings-security.xml`` and ``settings.xml`` as specified on the `Apache guide <http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env>`_. It can be tested with the following command:

    .. code-block::

        # You might need to export GPG_TTY=$(tty) to properly prompt for a passphrase
        mvn clean install -Papache-release

    - Have the build requirements for cpp and c_glib installed.
    - Set the JIRA_USERNAME and JIRA_PASSWORD environment variables
    - Set the ARROW_GITHUB_API_TOKEN environment variable to automatically create the verify release Pull Request.
    - Install ``en_US.UTF-8`` locale. You can confirm available locales by ``locale -a``.
    - Install Python 3 as python
    - Create dev/release/.env from dev/release/.env.example. See the comments in dev/release/.env.example how to set each variable.
    - Request to the Apache INFRA group to be aadded to `Bintray members <https://bintray.com/apache/>`_.
    - Setup :ref:`Crossbow<Crossbow>` as defined.
    - Have Docker and docker-compose installed.


Before creating a Release Candidate
===================================

Ensure local tags are removed, gpg-agent is set and JIRA tickets are correctly assigned.

.. code-block::

    # Delete the local tag for RC1 or later
    git tag -d apache-arrow-<version>
    
    # Setup gpg agent for signing artifacts
    source dev/release/setup-gpg-agent.sh
    
    # Curate the release
    # The end of the generated report shows the JIRA tickets with wrong version number assigned.
    archery release curate <version>


Creating a Release Candidate
============================

These are the different steps that are required to create a Release Candidate.

For the initial Release Candidate, we will create a maintenance branch from master.
Follow up Release Candidates will update the maintenance branch by cherry-picking
specific commits.

We have implemented a Feature Freeze policy between Release Candidates.
This means that, in general, we should only add bug fixes between Release Candidates.
In rare cases, critical features can be added between Release Candidates, if
there is community consensus.

Create or update the corresponding maintenance branch
-----------------------------------------------------

.. tab-set::

   .. tab-item:: Initial Release Candidate

      .. code-block::

            # Execute the following from an up to date master branch.
            # This will create a branch locally called maint-X.Y.Z.
            # X.Y.Z corresponds with the Major, Minor and Patch version number
            # of the release respectively. As an example 9.0.0
            archery release --jira-cache /tmp/jiracache cherry-pick X.Y.Z --execute
            # Push the maintenance branch to the remote repository
            git push -u apache maint-X.Y.Z

   .. tab-item:: Follow up Release Candidates

      .. code-block::

            # First run in dry-mode to see which commits will be cherry-picked.
            # If there are commits that we don't want to get applied ensure the version on
            # JIRA is set to the following release.
            archery release --jira-cache /tmp/jiracache cherry-pick X.Y.Z --continue
            # Update the maintenance branch with the previous commits
            archery release --jira-cache /tmp/jiracache cherry-pick X.Y.Z --continue --execute
            # Push the updated maintenance branch to the remote repository
            git push -u apache maint-X.Y.Z

Create the Release Candidate branch from the updated maintenance branch
-----------------------------------------------------------------------

.. code-block::

    # Start from the updated maintenance branch.
    git checkout maint-X.Y.Z
    
    # The following script will create a branch for the Release Candidate,
    # place the necessary commits updating the version number and then create a git tag
    # on OSX use gnu-sed with homebrew: brew install gnu-sed (and export to $PATH)
    #
    # <rc-number> starts at 0 and increments every time the Release Candidate is burned
    # so for the first RC this would be: dev/release/01-prepare.sh 4.0.0 5.0.0 0
    dev/release/01-prepare.sh <version> <next-version> <rc-number>
    
    # Push the release tag (for RC1 or later the --force flag is required)
    git push -u apache apache-arrow-<version>
    # Push the release candidate branch in order to trigger verification jobs later
    git push -u apache release-<version>-rc<rc-number>

Build source and binaries and submit them
-----------------------------------------

.. code-block::

    # Build the source release tarball and create Pull Request with verification tasks
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
    # On macOS the only way I could get this to work was running "echo "UPDATESTARTUPTTY" | gpg-connect-agent" before running this comment
    # otherwise I got errors referencing "ioctl" errors.
    dev/release/05-binary-upload.sh <version> <rc-number>
    
    # Sign and upload the Java artifacts
    #
    # Note that you need to press the "Close" button manually by Web interfacec
    # after you complete the script:
    #   https://repository.apache.org/#stagingRepositories
    dev/release/06-java-upload.sh <version> <rc-number>

Verify the Release
------------------

.. code-block::

    # Once the automatic verification has passed merge the Release Candidate's branch to the maintenance branch
    git checkout maint-<version>
    git merge release-<version>-rc<rc-number>
    git push apache maint-<version>
    
    # Start the vote thread on dev@arrow.apache.org
    # To regenerate the email template use
    SOURCE_DEFAULT=0 SOURCE_VOTE=1 dev/release/02-source.sh <version> <rc-number>

Voting and approval
===================

Start the vote thread on dev@arrow.apache.org and supply instructions for verifying the integrity of the release.
Approval requires a net of 3 +1 votes from PMC members. A release cannot be vetoed.

Post-release tasks
==================

After the release vote, we must undertake many tasks to update source artifacts, binary builds, and the Arrow website.

Be sure to go through on the following checklist:

#. Make the released version as "RELEASED" on JIRA
#. Make the CPP PARQUET related version as "RELEASED" on JIRA
#. Start the new version on JIRA on the ARROW project
#. Start the new version on JIRA for the related CPP PARQUET version
#. Merge changes on release branch to maintenance branch for patch releases
#. Upload source
#. Upload binaries
#. Update website
#. Update Homebrew packages
#. Update MSYS2 package
#. Upload RubyGems
#. Upload JS packages
#. Upload C# packages
#. Update conda recipes
#. Upload wheels/sdist to pypi
#. Publish Maven artifacts
#. Update R packages
#. Update vcpkg port
#. Bump versions
#. Update tags for Go modules
#. Update docs
#. Remove old artifacts

.. dropdown:: Marking the released version as "RELEASED" on JIRA
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    Open https://issues.apache.org/jira/plugins/servlet/project-config/ARROW/administer-versions

    Click "..." for the release version in "Actions" column

    Select "Release"

    Set "Release date"

    Click "Release" button

.. dropdown:: Starting the new version on JIRA
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    Open https://issues.apache.org/jira/plugins/servlet/project-config/ARROW/administer-versions

    Click "..." for the next version in "Actions" column

    Select "Edit"

    Set "Start date"

    Click "Save" button

.. dropdown:: Updating the Arrow website
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    Fork the `arrow-site repository <https://github.com/apache/arrow-site>`_ and clone it next to the arrow repository.

    Generate the release note:

    .. code-block::
    
        # dev/release/post-03-website 0.13.0 0.14.0
        dev/release/post-03-website <previous-version> <version>
    
    Create a pull-request and a Jira with the links the script shows at the end.

.. dropdown:: Uploading source release artifacts to SVN
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    A PMC member must commit the source release artifacts to SVN:

    .. code-block::
    
        # dev/release/post-01-upload.sh 0.1.0 0
        dev/release/post-01-upload.sh <version> <rc>

.. dropdown:: Uploading binary release artifacts to Artifactory
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    A PMC member must upload the binary release artifacts to Artifactory:

    .. code-block::
    
        # dev/release/post-02-binary.sh 0.1.0 0
        dev/release/post-02-binary.sh <version> <rc number>

.. dropdown:: Announcing release
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    Add relevant release data for Arrow to `Apache reporter <https://reporter.apache.org/addrelease.html?arrow>`_.

    Write a release announcement (see `example <https://lists.apache.org/thread/6rkjwvyjjfodrxffllh66pcqnp729n3k>`_) and send to announce@apache.org and dev@arrow.apache.org.

    The announcement to announce@apache.org must be sent from your apache.org e-mail address to be accepted.

.. dropdown:: Generating new API documentations and update the website
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

    The API documentation for C++, C Glib, Python, Java, and JavaScript can be generated via a Docker-based setup.
    To generate the API documentation run the following command:

    .. code-block::
    
        # preferred to have a cuda capable device with a recent docker version to generate the cuda docs as well
        # if you don't have an nvidia GPU please ask for help on the mailing list
        dev/release/post-08-docs.sh <version>
        
        # without a cuda device it's still possible to generate the apidocs with the following archery command
        archery docker run -v "${ARROW_SITE_DIR}/docs:/build/docs" -e ARROW_DOCS_VERSION="${version}" ubuntu-docs  
    
    Note, that on a case insensitive filesystem sphinx generate duplicate filenames, so there can be missing links on the documentation page. Please use a system (preferably Linux) to execute the command above. 

    This script assumes that the arrow-site repository is cloned next to the arrow source repository. Please note that most of the software must be built in order to create the documentation, so this step may take some time to run, especially the first time around as the Docker container will also have to be built.

