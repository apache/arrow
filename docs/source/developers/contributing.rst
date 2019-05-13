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

.. _contributing:

***********************
Contribution Guidelines
***********************

There are many ways to contribute to Apache Arrow:

* Contributing code (we call them "patches")
* Writing documentation (another form of code, in a way)
* Participating in discussions on `JIRA <https://issues.apache.org/jira/projects/ARROW/issues>`_ or the `mailing list <https://lists.apache.org/list.html?dev@arrow.apache.org>`_
* Helping users of the libraries
* Reporting bugs and asking questions

Mailing List
============

Projects in The Apache Software Foundation ("the ASF") use public, archived
mailing lists to create a public record of each project's development
activities and decision-making process. As such, all contributors generally
must be subscribed to the dev@arrow.apache.org mailing list to participate in
the community.

Note that you must be subscribed to the mailing list in order to post to it. To
subscribe, send a blank email to dev-subscribe@arrow.apache.org.

Mailing list archives can be found `here <https://lists.apache.org/list.html?dev@arrow.apache.org>`_.

Issue Tracking
==============

We use the `ASF JIRA <https://issues.apache.org/jira/projects/ARROW/issues>`_
to manage our development "todo" list and to maintain changelogs for releases.
In addition, the project's `Confluence site <https://cwiki.apache.org/confluence/display/ARROW>`_
has some useful higher-level views of the JIRA issues.

To create a JIRA issue, you'll need to have an account on the ASF JIRA, which
you can `sign yourself up for <https://issues.apache.org/jira/secure/Signup!default.jspa>`_. No
additional permissions are needed to create issues. Only once you are involved
in the project and want to do more on JIRA, such as assign yourself an issue,
will you need "Contributor" permissions on the Apache Arrow JIRA. To get this
role, ask on the mailing list for a project maintainer's help.

When reporting a new issue, follow these conventions to help make sure the
right people see it:

* If the issue is specific to a language binding or other key component, prefix the issue name with it, like ``[Python] issue name``.
* If you're reporting something that used to work in a previous version but doesn't work in the current release, you can add the "Affects version" field. For feature requests and other proposals, "Affects version" isn't appropriate.

Project maintainers may later tweak formatting and labels to help improve their
visibility. They may add a "Fix version" to indicate that they're considering
it for inclusion in the next release, though adding that tag is not a
commitment that it will be done in the next release.

GitHub issues
-------------

We support `GitHub issues <https://github.com/apache/arrow/issues>`_ as a
lightweight way to ask questions and engage with
the Arrow developer community. We use JIRA for maintaining a queue of
development work and as the public record for work on the project. So, feel
free to open GitHub issues, but bugs and feature requests will eventually need
to end up in JIRA, either before or after completing a pull request. Don't be
surprised if you are immediately asked by a project maintainer to open a JIRA
issue.

How to contribute patches
=========================

We prefer to receive contributions in the form of GitHub pull requests. Please
send pull requests against the `github.com/apache/arrow
<https://github.com/apache/arrow>`_ repository following the procedure below.

If you are looking for some ideas on what to contribute, check out the JIRA
issues for the Apache Arrow project. Comment on the issue and/or contact
dev@arrow.apache.org with your questions and ideas.

If you’d like to report a bug but don’t have time to fix it, you can still post
it on JIRA, or email the mailing list dev@arrow.apache.org.

To contribute a patch:

* Break your work into small, single-purpose patches if possible. It’s much
  harder to merge in a large change with a lot of disjoint features.
* If one doesn't already exist, create a JIRA for your patch on the
  `Arrow Project JIRA <https://issues.apache.org/jira/projects/ARROW/issues>`_.
* Submit the patch as a GitHub pull request against the master branch. For a
  tutorial, see the GitHub guides on `forking a repo <https://help.github.com/en/articles/fork-a-repo>`_
  and `sending a pull request <https://help.github.com/en/articles/creating-a-pull-request-from-a-fork>`_.
  So that your pull request syncs with the JIRA issue, prefix your pull request
  name with the JIRA issue id (ex:
  `ARROW-767: [C++] Filesystem abstraction <https://github.com/apache/arrow/pull/4225>`_).
* Make sure that your code passes the unit tests. You can find instructions how
  to run the unit tests for each Arrow component in its respective README file.
* Add new unit tests for your code.

Thank you in advance for your contributions!
