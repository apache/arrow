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
* Participating in discussions on JIRA or the mailing list
* Helping users of the libraries
* Reporting bugs and asking questions

Mailing Lists and Issue Tracker
===============================

Projects in The Apache Software Foundation ("the ASF") use public, archived
mailing lists to create a public record of each project's development
activities and decision making process. As such, all contributors generally
must be subscribed to the dev@arrow.apache.org mailing list to participate in
the community.

Note that you must be subscribed to the mailing list in order to post to it. To
subscribe, send a blank email to dev-subscribe@arrow.apache.org.

We use the `ASF JIRA <https://issues.apache.org/jira>`_ to manage our
development "todo" list and to maintain changelogs for releases. You must
create an account and be added as a "Contributor" to Apache Arrow to be able to
assign yourself issues. Any project maintainer will be able to help you with
this one-time setup.

GitHub issues
-------------

We support GitHub issues as a lightweight way to ask questions and engage with
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
* Create a JIRA for your patch on the Arrow Project JIRA.
* Submit the patch as a GitHub pull request against the master branch. For a
  tutorial, see the GitHub guides on forking a repo and sending a pull
  request. Prefix your pull request name with the JIRA name (ex:
  https://github.com/apache/arrow/pull/240).
* Make sure that your code passes the unit tests. You can find instructions how
  to run the unit tests for each Arrow component in its respective README file.
* Add new unit tests for your code.

Thank you in advance for your contributions!
