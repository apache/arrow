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

.. _bug-reports:

********************************
Bug reports and feature requests
********************************

Arrow relies upon user feedback to identify defects and improvement
opportunities. All users are encouraged to participate by creating bug reports
and feature requests or commenting on existing issues. Even if you cannot
contribute solutions to the issues yourself, your feedback helps us understand
problems and prioritize work to improve the libraries.

.. _github_issues:

GitHub issues
=============

The Arrow project uses `GitHub issues <https://github.com/apache/arrow/issues>`_
to track issues - both bug reports and feature requests.

.. _creating-issues:

Creating issues
===============

Apache Arrow relies upon community contributions to address reported bugs and
feature requests. As with most software projects, contributor time and
resources are finite. The following guidelines aim to produce high-quality
bug reports and feature requests, enabling community contributors to respond
to more issues, faster:

.. _check-existing-issues:

Check existing issues
+++++++++++++++++++++

Before you create a new issue, we recommend you first
`search <https://github.com/apache/arrow/issues>`_
for unresolved existing issues identifying the same problem or feature request.

.. _describe-issue:

Issue description
+++++++++++++++++

A clear description of the problem or requested feature is the most important
element of any issue.  An effective description helps developers understand
and efficiently engage on reported issues, and may include the following:

* **Clear, minimal steps to reproduce the issue, with as few non-Arrow
  dependencies as possible.** If there's a problem on reading a file, try to
  provide as small of an example file as possible, or code to create one.
  If your bug report says "it crashes trying to read my file, but I can't
  share it with you," it's really hard for us to debug.
* Any relevant operating system, language, and library version information
* If it isn't obvious, clearly state the expected behavior and what actually
  happened.
* Avoid overloading a single issue with multiple problems or feature requests.
  Each issue should deal with a single bug or feature.

If a developer can't get a failing unit test, they won't be able to know that
the issue has been identified, and they won't know when it has been fixed.
Try to anticipate the questions you might be asked by someone working to
understand the issue and provide those supporting details up front.

Examples of good bug reports are found below:

.. tab-set::

   .. tab-item:: Python

      The ``print`` method of a timestamp with timezone errors:

      .. code-block:: python

         import pyarrow as pa

         a = pa.array([0], pa.timestamp('s', tz='+02:00'))

         print(a) # representation not correct?
         # <pyarrow.lib.TimestampArray object at 0x7f834c7cb9a8>
         # [
         #  1970-01-01 00:00:00
         # ]

         print(a[0])
         #Traceback (most recent call last):
         #  File "<stdin>", line 1, in <module>
         #  File "pyarrow/scalar.pxi", line 80, in pyarrow.lib.Scalar.__repr__
         #  File "pyarrow/scalar.pxi", line 463, in pyarrow.lib.TimestampScalar.as_py
         #  File "pyarrow/scalar.pxi", line 393, in pyarrow.lib._datetime_from_int
         #ValueError: fromutc: dt.tzinfo is not self

   .. tab-item:: R

      Error when reading a CSV file with ``col_types`` option ``"T"`` or ``"t"`` when source data is in millisecond precision:

      .. code-block:: R

         library(arrow, warn.conflicts = FALSE)
         tf <- tempfile()
         write.csv(data.frame(x = '2018-10-07 19:04:05.005'), tf, row.names = FALSE)

         # successfully read in file
         read_csv_arrow(tf, as_data_frame = TRUE)
         #> # A tibble: 1 × 1
         #>   x
         #>   <dttm>
         #> 1 2018-10-07 20:04:05

         # the unit here is seconds - doesn't work
         read_csv_arrow(
           tf,
           col_names = "x",
           col_types = "T",
           skip = 1
         )
         #> Error in `handle_csv_read_error()`:
         #> ! Invalid: In CSV column #0: CSV conversion error to timestamp[s]: invalid value '2018-10-07 19:04:05.005'

         # the unit here is ms - doesn't work
         read_csv_arrow(
           tf,
           col_names = "x",
           col_types = "t",
           skip = 1
         )
         #> Error in `handle_csv_read_error()`:
         #> ! Invalid: In CSV column #0: CSV conversion error to time32[ms]: invalid value '2018-10-07 19:04:05.005'

         # the unit here is inferred as ns - does work!
         read_csv_arrow(
           tf,
           col_names = "x",
           col_types = "?",
           skip = 1,
           as_data_frame = FALSE
         )
         #> Table
         #> 1 rows x 1 columns
         #> $x <timestamp[ns]>

Other resources for producing useful bug reports:

* `Python: Craft Minimal Bug Reports by Matthew Rocklin <https://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports>`_
* `R: Tidyverse: Make a reprex <https://www.tidyverse.org/help/#reprex>`_
* `R: Tidyverse's Reprex do's and don'ts <https://reprex.tidyverse.org/articles/reprex-dos-and-donts.html>`_
* `Mozilla's bug-reporting guidelines <https://developer.mozilla.org/en-US/docs/Mozilla/QA/Bug_writing_guidelines>`_

.. _identify-component:

Identify Arrow component
++++++++++++++++++++++++

Arrow is an expansive project supporting many languages and organized into a
number of components. Identifying the affected component(s) helps new issues
get attention from appropriate contributors.

* **Component label**, which can be added by a committer of the Apache Arrow
  project, is used to indicate the area of the project that your issue pertains
  to (for example "Component: Python" or "Component: C++").
* Prefix the issue title with the component name in brackets, for example
  ``[Python] issue summary`` ; this helps when navigating lists of open issues,
  and it also makes our changelogs more readable. Most prefixes are exactly the
  same as the **Component** name, with the following exceptions:

  * **Component:** Continuous Integration — **Summary prefix:** [CI]
  * **Component:** Developer Tools — **Summary prefix:** [Dev]
  * **Component:** Documentation — **Summary prefix:** [Docs]

.. _issue-lifecycle:

Issue lifecycle
===============

Both bug reports and feature requests follow a defined lifecycle. If an issue
is currently worked on, it should have a developer assigned. When an issue has
reached a terminal status, it is closed with one of two outcomes:

* **Closed as completed** - indicates the issue is complete; the PR that
  resolved the issue should have been automatically linked by GitHub
  (assuming the PR correctly mentioned the issue number).

  If you are merging a PR it is good practice to add a comment
  to the linked issue about which PR is resolving it. This way
  GitHub crates a notification for anybody that collaborated on
  the issue.

* **closed as not planned** - indicates the issue is closed and should
  not receive any further updates, but *without* action being taken.

.. _issue-assignment:

Issue assignment
++++++++++++++++

Assignment signals commitment to work on an issue, and contributors should
self-assign issues when that work starts. Anyone can now self-assign issues
by commenting ``take``.
