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
Report bugs and propose features
********************************

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
===================

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

.. _bug-report-tips:

Tips for successful bug reports
================================

No one likes having bugs in their software, and in an ideal world, all bugs
would get fixed as soon as they were reported. However, time and attention are
finite, especially in an open-source project where most contributors are
participating in their spare time. All contributors in Apache projects are
volunteers and act as individuals, even if they are contributing to the project
as part of their job responsibilities.

In order for your bug to get prompt
attention, there are things you can do to make it easier for contributors to
reproduce and fix it.
**When you're reporting a bug, please help us understand the issue by providing,
to the best of your ability,**

* **Clear, minimal steps to reproduce the issue, with as few non-Arrow
  dependencies as possible.** If there's a problem on reading a file, try to
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

Good reproducible examples or minimal bug reports can be found in next tabs:

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


Other resources:

* `Python: Craft Minimal Bug Reports by Matthew Rocklin <https://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports>`_
* `R: Tidyverse: Make a reprex <https://www.tidyverse.org/help/#reprex>`_
* `R: Tidyverse's Reprex do's and don'ts <https://reprex.tidyverse.org/articles/reprex-dos-and-donts.html>`_
* `Mozilla's bug-reporting guidelines <https://developer.mozilla.org/en-US/docs/Mozilla/QA/Bug_writing_guidelines>`_
