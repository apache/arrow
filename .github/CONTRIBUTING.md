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

## Contributing to Apache Arrow

There are many ways to contribute to Apache Arrow:

* Contributing code (we call them "patches")
* Writing documentation (another form of code, in a way)
* Participating in discussions on JIRA or the mailing list
* Helping users of the libraries

## Reporting bugs and asking questions

We support GitHub issues as a lightweight way to ask questions and engage with
the Arrow developer community. We use [JIRA][3] for maintaining a queue of
development work and as the public record for work on the project. So, feel
free to open GitHub issues, but bugs and feature requests will eventually need
to end up in JIRA, either before or after completing a pull request.

## How to contribute patches

We prefer to receive contributions in the form of GitHub pull requests. Please
send pull requests against the [github.com/apache/arrow][4] repository following
the procedure below.

If you are looking for some ideas on what to contribute, check out the [JIRA
issues][3] for the Apache Arrow project. Comment on the issue and/or contact
[dev@arrow.apache.org](https://lists.apache.org/list.html?dev@arrow.apache.org)
with your questions and ideas.

If you’d like to report a bug but don’t have time to fix it, you can still post
it on JIRA, or email the mailing list
[dev@arrow.apache.org](https://lists.apache.org/list.html?dev@arrow.apache.org)

To contribute a patch:

1. Break your work into small, single-purpose patches if possible. It’s much
harder to merge in a large change with a lot of disjoint features.
2. If one doesn't already exist, create a JIRA for your patch on the [Arrow Project
JIRA](https://issues.apache.org/jira/browse/ARROW).
3. Submit the patch as a GitHub pull request against the master branch. For a
tutorial, see the GitHub guides on [forking a repo](https://help.github.com/en/articles/fork-a-repo)
and [sending a pull request](https://help.github.com/en/articles/creating-a-pull-request-from-a-fork). So that your pull request syncs with the JIRA issue, prefix your pull request
name with the JIRA issue id (ex: [ARROW-767: [C++] Filesystem abstraction](https://github.com/apache/arrow/pull/4225))
4. Make sure that your code passes the unit tests. You can find instructions
how to run the unit tests for each Arrow component in its respective README
file.
5. Add new unit tests for your code.

Thank you in advance for your contributions!

[1]: mailto:dev-subscribe@arrow.apache.org
[2]: https://github.com/apache/arrow/tree/master/format
[3]: https://issues.apache.org/jira/browse/ARROW
[4]: https://github.com/apache/arrow
