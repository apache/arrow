<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

## Apache Arrow

<table>
  <tr>
    <td>Build Status</td>
    <td>
    <a href="https://travis-ci.org/apache/arrow">
    <img src="https://travis-ci.org/apache/arrow.svg?branch=master" alt="travis build status" />
    </a>
    </td>
  </tr>
</table>

#### Powering Columnar In-Memory Analytics

Arrow is a set of technologies that enable big-data systems to process and move data fast.

Initial implementations include:

 - [The Arrow Format](https://github.com/apache/arrow/tree/master/format)
 - [Java implementation](https://github.com/apache/arrow/tree/master/java)
 - [C++ implementation](https://github.com/apache/arrow/tree/master/cpp)
 - [Python interface to C++ libraries](https://github.com/apache/arrow/tree/master/python)

Arrow is an [Apache Software Foundation](www.apache.org) project. Learn more at
[arrow.apache.org](http://arrow.apache.org).

#### What's in the Arrow libraries?

The reference Arrow implementations contain a number of distinct software
components:

- Columnar vector and table-like containers (similar to data frames) supporting
  flat or nested types
- Fast, language agnostic metadata messaging layer (using Google's Flatbuffers
  library)
- Reference-counted off-heap buffer memory management, for zero-copy memory
  sharing and handling memory-mapped files
- Low-overhead IO interfaces to files on disk, HDFS (C++ only)
- Self-describing binary wire formats (streaming and batch/file-like) for
  remote procedure calls (RPC) and
  interprocess communication (IPC)
- Integration tests for verifying binary compatibility between the
  implementations (e.g. sending data from Java to C++)
- Conversions to and from other in-memory data structures (e.g. Python's pandas
  library)

#### Getting involved

Right now the primary audience for Apache Arrow are the developers of data
systems; most people will use Apache Arrow indirectly through systems that use
it for internal data handling and interoperating with other Arrow-enabled
systems.

Even if you do not plan to contribute to Apache Arrow itself or Arrow
integrations in other projects, we'd be happy to have you involved:

- Join the mailing list: send an email to
  [dev-subscribe@arrow.apache.org][1]. Share your ideas and use cases for the
  project.
- [Follow our activity on JIRA][3]
- [Learn the format][2]
- Contribute code to one of the reference implementations

[1]: mailto:dev-subscribe@arrow.apache.org
[2]: https://github.com/apache/arrow/tree/master/format
[3]: https://issues.apache.org/jira/browse/ARROW
