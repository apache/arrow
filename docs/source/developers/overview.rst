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

.. _contrib-overview:

*********************
Contributing Overview
*********************

.. _git-conventions:

Local git conventions
=====================

If you are tracking the Arrow source repository locally, here is a
checklist for using ``git``:

* Work off of your **personal fork** of ``apache/arrow`` and submit pull requests
  "upstream".
* Keep your fork's **main branch synced** with ``upstream/main``.
* **Develop on branches**, rather than your own "main" branch.
* It does not matter what you call your branch. Some people like to use the GitHub
  issue number as branch name, others use descriptive names.
* **Sync your branch** with ``upstream/main`` **regularly**, as many commits are
  merged to main every day.
* It is recommended to use ``git rebase`` rather than ``git merge``.
* In case there are conflicts, and your local commit history has multiple commits,
  you may simplify the conflict resolution process by **squashing your local commits
  into a single commit**. Preserving the commit history isn't as important because
  when your feature branch is merged upstream, a squash happens automatically.

  .. dropdown:: How to squash local commits?
    :animate: fade-in-slide-down
    :class-container: sd-shadow-md

    Abort the rebase with:

    .. code:: console

       $ git rebase --abort

    Following which, the local commits can be squashed interactively by running:

    .. code:: console

       $ git rebase --interactive ORIG_HEAD~n

    Where ``n`` is the number of commits you have in your local branch.  After the squash,
    you can try the merge again, and this time conflict resolution should be relatively
    straightforward.

    Once you have an updated local copy, you can push to your remote repo.  Note, since your
    remote repo still holds the old history, you would need to do a force push.  Most pushes
    should use ``--force-with-lease``:

    .. code:: console

       $ git push --force-with-lease origin branch

    The option ``--force-with-lease`` will fail if the remote has commits that are not available
    locally, for example if additional commits have been made by a colleague.  By using
    ``--force-with-lease`` instead of ``--force``, you ensure those commits are not overwritten
    and can fetch those changes if desired.
    
  .. dropdown:: Setting rebase to be default
    :animate: fade-in-slide-down
    :class-container: sd-shadow-md

    If you set the following in your repo's ``.git/config``, the ``--rebase`` option can be
    omitted from the ``git pull`` command, as it is implied by default.

    .. code:: console

       [pull]
             rebase = true


.. _pull-request-and-review:

Pull request and review
=======================

When contributing a patch, use this list as a checklist of Apache Arrow workflow:

* Submit the patch as a **GitHub pull request** against the **main branch**.
* So that your pull request syncs with the GitHub issue, **prefix your pull request
  title with the GitHub issue id** (ex:
  `GH-14866: [C++] Remove internal GroupBy implementation <https://github.com/apache/arrow/pull/14867>`_).
  Similarly **prefix your pull request name with the JIRA issue id** (ex:
  `ARROW-767: [C++] Filesystem abstraction <https://github.com/apache/arrow/pull/4225>`_)
  in case the issue is still located in Jira.
* Give the pull request a **clear, brief description**: when the pull request is
  merged, this will be retained in the extended commit message.
* Make sure that your code **passes the unit tests**. You can find instructions how
  to run the unit tests for each Arrow component in its respective README file.

Core developers and others with a stake in the part of the project your change
affects will review, request changes, and hopefully indicate their approval
in the end. To make the review process smooth for everyone, try to

* **Break your work into small, single-purpose patches if possible.**

  It’s much harder to merge in a large change with a lot of disjoint features,
  and particularly if you're new to the project, smaller changes are much easier
  for maintainers to accept.

* **Add new unit tests for your code.**
* **Follow the style guides** for the part(s) of the project you're modifying.

  Some languages (C++ and Python, for example) run a lint check in
  continuous integration. For all languages, see their respective developer
  documentation and READMEs for style guidance.

* Try to make it look as if the codebase has a single author,
  and emulate any conventions you see, whether or not they are officially
  documented or checked.

When tests are passing and the pull request has been approved by the interested
parties, a `committer <https://arrow.apache.org/committers/>`_
will merge the pull request. This is done with a
**command-line utility that does a squash merge**.

.. dropdown:: Details on squash merge
  :animate: fade-in-slide-down
  :class-container: sd-shadow-md

  A pull request is merged with a squash merge so that all of your commits will be
  registered as a single commit to the main branch; this simplifies the
  connection between GitHub issues and commits, makes it easier to bisect
  history to identify where changes were introduced, and helps us be able to
  cherry-pick individual patches onto a maintenance branch.

  Your pull request will appear in the GitHub interface to have been "merged".
  In the commit message of that commit, the merge tool adds the pull request
  description, a link back to the pull request, and attribution to the contributor
  and any co-authors.

.. Section on Experimental repositories:

.. include:: experimental_repos.rst

.. _specific-features:

Guidance for specific features
==============================

From time to time the community has discussions on specific types of features
and improvements that they expect to support.  This section outlines decisions
that have been made in this regard.

Endianness
++++++++++

The Arrow format allows setting endianness.  Due to the popularity of
little endian architectures most of implementation assume little endian by
default. There has been some  effort to support big endian platforms as well.
Based on a `mailing-list discussion
<https://mail-archives.apache.org/mod_mbox/arrow-dev/202009.mbox/%3cCAK7Z5T--HHhr9Dy43PYhD6m-XoU4qoGwQVLwZsG-kOxXjPTyZA@mail.gmail.com%3e>`__,
the requirements for a new platform are:

1. A robust (non-flaky, returning results in a reasonable time) Continuous
   Integration setup.
2. Benchmarks for performance critical parts of the code to demonstrate
   no regression.

Furthermore, for big-endian support, there are two levels that an
implementation can support:

1. Native endianness (all Arrow communication happens with processes of the
   same endianness).  This includes ancillary functionality such as reading
   and writing various file formats, such as Parquet.
2. Cross endian support (implementations will do byte reordering when
   appropriate for :ref:`IPC <format-ipc>` and :ref:`Flight <flight-rpc>`
   messages).

The decision on what level to support is based on maintainers' preferences for
complexity and technical risk.  In general all implementations should be open
to native endianness support (provided the CI and performance requirements
are met).  Cross endianness support is a question for individual maintainers.

The current implementations aiming for cross endian support are:

1. C++

Implementations that do not intend to implement cross endian support:

1. Java

For other libraries, a discussion to gather consensus on the mailing-list
should be had before submitting PRs.

