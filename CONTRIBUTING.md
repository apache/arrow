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

The Arrow project uses GitHub as a bug tracker.  To report a bug, sign in to
your GitHub account, navigate to [GitHub issues](https://github.com/apache/arrow/issues)
and click on **New issue** .

To be assigned to an issue, add a comment "take" to that issue.

Before you create a new bug entry, we recommend you first search among existing
Arrow issues in
[Jira](https://issues.apache.org/jira/issues/?jql=project%20%3D%20ARROW%20AND%20status%20%3D%20Open)
or [GitHub](https://github.com/apache/arrow/issues).

We conventionally prefix the issue title with the component
name in brackets, such as "[C++][Python] Ensure no validity bitmap in
UnionArray::SetData", so as to make lists more easy to navigate, and
we'd be grateful if you did the same.

## Did you write a patch that fixes a bug or brings an improvement?

First create a GitHub issue as described above, selecting **Bug Report** or
**Enhancement Request**. Then, submit your changes as a GitHub Pull Request.
We'll ask you to prefix the pull request title with the GitHub issue number
and the component name in brackets. (for example: "GH-14736: [C++][Python]
Ensure no validity bitmap in UnionArray::SetData"). Respecting this convention
makes it easier for us to process the backlog of submitted Pull Requests.

### Minor Fixes

Any functionality change should have a GitHub issue opened. For minor changes that
affect documentation, you do not need to open up a GitHub issue. Instead you can
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

Please read our [development documentation](https://arrow.apache.org/docs/developers/contributing.html)
or look through the [New Contributor's Guide](https://arrow.apache.org/docs/developers/guide/index.html).
