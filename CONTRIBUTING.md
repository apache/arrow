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

# How to contribute to Apache Arrow

## Did you find a bug?

The Arrow project uses JIRA as a bug tracker.  To report a bug, you'll have
to first create an account on the
[Apache Foundation JIRA](https://issues.apache.org/jira/).  The JIRA server
hosts bugs and issues for multiple Apache projects.  The JIRA project name
for Arrow is "ARROW".

To be assigned to an issue, ask an Arrow JIRA admin to go to
[Arrow Roles](https://issues.apache.org/jira/plugins/servlet/project-config/ARROW/roles),
click "Add users to a role," and add you to the "Contributor" role.  Most
committers are authorized to do this; if you're a committer and aren't
able to load that project admin page, have someone else add you to the
necessary role.

Before you create a new bug entry, we recommend you first
[search](https://issues.apache.org/jira/projects/ARROW/issues/ARROW-5140?filter=allopenissues)
among existing Arrow issues.

When you create a new JIRA entry, please don't forget to fill the "Component"
field.  Arrow has many subcomponents and this helps triaging and filtering
tremendously.  Also, we conventionally prefix the issue title with the component
name in brackets, such as "[C++] Crash in Array::Frobnicate()", so as to make
lists more easy to navigate, and we'd be grateful if you did the same.

## Did you write a patch that fixes a bug or brings an improvement?

First create a JIRA entry as described above.  Then, submit your changes
as a GitHub Pull Request.  We'll ask you to prefix the pull request title
with the JIRA issue number and the component name in brackets.
(for example: "ARROW-2345: [C++] Fix crash in Array::Frobnicate()").
Respecting this convention makes it easier for us to process the backlog
of submitted Pull Requests.

### Minor Fixes

Any functionality change should have a JIRA opened.  For minor changes that
affect documentation, you do not need to open up a JIRA.  Instead you can
prefix the title of your PR with "MINOR: " if meets the following guidelines:

*  Grammar, usage and spelling fixes that affect no more than 2 files
*  Documentation updates affecting no more than 2 files and not more
   than 500 words.

## Do you want to propose a significant new feature or an important refactoring?

We ask that all discussions about major changes in the codebase happen
publicly on the [arrow-dev mailing-list](https://mail-archives.apache.org/mod_mbox/arrow-dev/).

## Do you have questions about the source code, the build procedure or the development process?

You can also ask on the mailing-list, see above.

## Further information

Please read our [development documentation](https://arrow.apache.org/docs/developers/contributing.html).
