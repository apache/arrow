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

.. highlight:: console

.. _contributing:

****************************
Contributing to Apache Arrow
****************************

Thanks for your interest in the Apache Arrow project. Arrow is a large project
and may seem overwhelming when you're first getting involved.
Contributing code is great, but that's probably not the first place to start.
There are lots of ways to make valuable contributions to the project and
community.

This page provides some orientation for how to get involved. It also offers
some recommendations on how to get best results when engaging with the
community.

Code of Conduct
===============

All participation in the Apache Arrow project is governed by the ASF's
`Code of Conduct <https://www.apache.org/foundation/policies/conduct.html>`_.

Join the mailing lists
======================

A good first step to getting involved in the Arrow project is to join the
mailing lists and participate in discussions where you can.
Projects in The Apache Software Foundation ("the ASF") use public, archived
mailing lists to create a public record of each project's development
activities and decision-making process.
While lacking the immediacy of chat or other forms of communication,
the mailing lists give participants the opportunity to slow down and be
thoughtful in their responses, and they help developers who are spread across
many timezones to participate more equally.

See `the community page <https://arrow.apache.org/community/>`_ for links to
subscribe to the mailing lists and to view archives.

Report bugs and propose features
================================

Using the software and sharing your experience is a very helpful contribution
itself. Those who actively develop Arrow need feedback from users on what
works and what doesn't. Alerting us to unexpected behavior and missing features,
even if you can't solve the problems yourself, help us understand and prioritize
work to improve the libraries.

We use `JIRA <https://issues.apache.org/jira/projects/ARROW/issues>`_
to manage our development "todo" list and to maintain changelogs for releases.
In addition, the project's `Confluence site <https://cwiki.apache.org/confluence/display/ARROW>`_
has some useful higher-level views of the JIRA issues.

To create a JIRA issue, you'll need to have an account on the ASF JIRA, which
you can `sign yourself up for <https://issues.apache.org/jira/secure/Signup!default.jspa>`_.
The JIRA server hosts bugs and issues for multiple Apache projects. The JIRA
project name for Arrow is "ARROW".

You don't need any special permissions on JIRA to be able to create issues.
Once you are more involved in the project and want to do more on JIRA, such as
assign yourself an issue, you will need "Contributor" permissions on the
Apache Arrow JIRA. To get this role, ask on the mailing list for a project
maintainer's help.


.. _jira-tips:

Tips for using JIRA
+++++++++++++++++++

Before you create a new issue, we recommend you first
`search <https://issues.apache.org/jira/issues/?jql=project%20%3D%20ARROW%20AND%20resolution%20%3D%20Unresolved>`_
among existing Arrow issues.

When reporting a new issue, follow these conventions to help make sure the
right people see it:

* Use the **Component** field to indicate the area of the project that your
  issue pertains to (for example "Python" or "C++").
* Also prefix the issue title with the component name in brackets, for example
  ``[Python] issue name`` ; this helps when navigating lists of open issues,
  and it also makes our changelogs more readable. Most prefixes are exactly the 
  same as the **Component** name, with the following exceptions:

  * **Component:** Continuous Integration — **Summary prefix:** [CI]
  * **Component:** Developer Tools — **Summary prefix:** [Dev]
  * **Component:** Documentation — **Summary prefix:** [Docs]

* If you're reporting something that used to work in a previous version
  but doesn't work in the current release, you can add the "Affects version"
  field. For feature requests and other proposals, "Affects version" isn't
  appropriate.

Project maintainers may later tweak formatting and labels to help improve their
visibility. They may add a "Fix version" to indicate that they're considering
it for inclusion in the next release, though adding that tag is not a
commitment that it will be done in the next release.

Tips for successful bug reports
+++++++++++++++++++++++++++++++

No one likes having bugs in their software, and in an ideal world, all bugs
would get fixed as soon as they were reported. However, time and attention are
finite, especially in an open-source project where most contributors are
participating in their spare time. All contributors in Apache projects are
volunteers and act as individuals, even if they are contributing to the project
as part of their job responsibilities.

In order for your bug to get prompt
attention, there are things you can do to make it easier for contributors to
reproduce and fix it.
When you're reporting a bug, please help us understand the issue by providing,
to the best of your ability,

* Clear, minimal steps to reproduce the issue, with as few non-Arrow
  dependencies as possible. If there's a problem on reading a file, try to
  provide as small of an example file as possible, or code to create one.
  If your bug report says "it crashes trying to read my file, but I can't
  share it with you," it's really hard for us to debug.
* Any relevant operating system, language, and library version information
* If it isn't obvious, clearly state the expected behavior and what actually
  happened.

If a developer can't get a failing unit test, they won't be able to know that
the issue has been identified, and they won't know when it has been fixed.
Try to anticipate the questions you might be asked by someone working to
understand the issue and provide those supporting details up front.

Other resources:

* `Mozilla's bug-reporting guidelines <https://developer.mozilla.org/en-US/docs/Mozilla/QA/Bug_writing_guidelines>`_
* `Reprex do's and don'ts <https://reprex.tidyverse.org/articles/reprex-dos-and-donts.html>`_

Improve documentation
=====================

A great way to contribute to the project is to improve documentation. If you
found some docs to be incomplete or inaccurate, share your hard-earned knowledge
with the rest of the community.

Documentation improvements are also a great way to gain some experience with
our submission and review process, discussed below, without requiring a lot
of local development environment setup. In fact, many documentation-only changes
can be made directly in the GitHub web interface by clicking the "edit" button.
This will handle making a fork and a pull request for you.

Contribute code
===============

Code contributions, or "patches", are delivered in the form of GitHub pull
requests against the `github.com/apache/arrow
<https://github.com/apache/arrow>`_ repository.

Before starting
+++++++++++++++

You'll first need to select a JIRA issue to work on. Perhaps you're working on
one you reported yourself. Otherwise, if you're looking for something,
`search <https://issues.apache.org/jira/issues/?jql=project%20%3D%20ARROW%20AND%20resolution%20%3D%20Unresolved>`_
the open issues. Anything that's not in the "In Progress" state is fair game,
even if it is "Assigned" to someone, particularly if it has not been
recently updated. When in doubt, comment on the issue asking if they mind
if you try to put together a pull request; interpret no response to mean that
you're free to proceed.

Please do ask questions, either on the JIRA itself or on the dev mailing list,
if you have doubts about where to begin or what approach to take.
This is particularly a good idea if this is your first code contribution,
so you can get some sense of what the core developers in this part of the
project think a good solution looks like. For best results, ask specific,
direct questions, such as:

* Do you think $PROPOSED_APPROACH is the right one?
* In which file(s) should I be looking to make changes?
* Is there anything related in the codebase I can look at to learn?

If you ask these questions and do not get an answer, it is OK to ask again.

Pull request and review
+++++++++++++++++++++++

To contribute a patch:

* Submit the patch as a GitHub pull request against the master branch. For a
  tutorial, see the GitHub guides on `forking a repo <https://help.github.com/en/articles/fork-a-repo>`_
  and `sending a pull request <https://help.github.com/en/articles/creating-a-pull-request-from-a-fork>`_.
  So that your pull request syncs with the JIRA issue, prefix your pull request
  name with the JIRA issue id (ex:
  `ARROW-767: [C++] Filesystem abstraction <https://github.com/apache/arrow/pull/4225>`_).
* Give the pull request a clear, brief description: when the pull request is
  merged, this will be retained in the extended commit message.
* Make sure that your code passes the unit tests. You can find instructions how
  to run the unit tests for each Arrow component in its respective README file.

Core developers and others with a stake in the part of the project your change
affects will review, request changes, and hopefully indicate their approval
in the end. To make the review process smooth for everyone, try to

* Break your work into small, single-purpose patches if possible. It’s much
  harder to merge in a large change with a lot of disjoint features, and
  particularly if you're new to the project, smaller changes are much easier
  for maintainers to accept.
* Add new unit tests for your code.
* Follow the style guides for the part(s) of the project you're modifying.
  Some languages (C++ and Python, for example) run a lint check in
  continuous integration. For all languages, see their respective developer
  documentation and READMEs for style guidance. In general, try to make it look
  as if the codebase has a single author, and emulate any conventions you see,
  whether or not they are officially documented or checked.

When tests are passing and the pull request has been approved by the interested
parties, a `committer <https://arrow.apache.org/committers/>`_
will merge the pull request. This is done with a
command-line utility that does a squash merge, so all of your commits will be
registered as a single commit to the master branch; this simplifies the
connection between JIRA issues and commits, makes it easier to bisect
history to identify where changes were introduced, and helps us be able to
cherry-pick individual patches onto a maintenance branch.

A side effect of this way of
merging is that your pull request will appear in the GitHub interface to have
been "closed without merge". Do not be alarmed: if you look at the bottom, you
will see a message that says ``@user closed this in $COMMIT``. In the commit
message of that commit, the merge tool adds the pull request description, a
link back to the pull request, and attribution to the contributor and any
co-authors.

Local git conventions
+++++++++++++++++++++

If you are tracking the Arrow source repository locally, here are some tips
for using ``git``.

All Arrow contributors work off of their personal fork of ``apache/arrow``
and submit pull requests "upstream". Once you've cloned your fork of Arrow,
be sure to::

    $ git remote add upstream https://github.com/apache/arrow

to set the "upstream" repository.

You are encouraged to develop on branches, rather than your own "master" branch,
and it helps to keep your fork's master branch synced with ``upstream/master``.

To start a new branch, pull the latest from upstream first::

   $ git fetch upstream
   $ git checkout master
   $ git pull --ff-only upstream master
   $ git checkout -b $BRANCH

It does not matter what you call your branch. Some people like to use the JIRA
number as branch name, others use descriptive names.

Once you have a branch going, you should sync with ``upstream/master``
regularly, as many commits are merged to master every day.
It is recommended to use ``git rebase`` rather than ``git merge``.
To sync your local copy of a branch, you may do the following::

    $ git pull upstream $BRANCH --rebase

This will rebase your local commits on top of the tip of ``upstream/$BRANCH``.  In case
there are conflicts, and your local commit history has multiple commits, you may
simplify the conflict resolution process by squashing your local commits into a single
commit. Preserving the commit history isn't as important because when your
feature branch is merged upstream, a squash happens automatically.  If you choose this
route, you can abort the rebase with::

    $ git rebase --abort

Following which, the local commits can be squashed interactively by running::

    $ git rebase --interactive ORIG_HEAD~n

Where ``n`` is the number of commits you have in your local branch.  After the squash,
you can try the merge again, and this time conflict resolution should be relatively
straightforward.

If you set the following in your repo's ``.git/config``, the ``--rebase`` option can be
omitted from the ``git pull`` command, as it is implied by default. ::

    [pull]
            rebase = true

Once you have an updated local copy, you can push to your remote repo.  Note, since your
remote repo still holds the old history, you would need to do a force push. ::

    $ git push --force origin branch

*Note about force pushing to a branch that is being reviewed:* if you want reviewers to
look at your updates, please ensure you comment on the PR on GitHub as simply force
pushing does not trigger a notification in the GitHub user interface.

Also, once you have a pull request up, be sure you pull from ``origin``
before rebasing and force-pushing. Arrow maintainers can push commits directly
to your branch, which they sometimes do to help move a pull request along.
In addition, the GitHub PR "suggestion" feature can also add commits to
your branch, so it is possible that your local copy of your branch is missing
some additions.

.. include:: experimental_repos.rst

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
