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


.. SCOPE OF THIS SECTION
.. Concise tutorial on making a PR for a simple feature.


.. _r-tutorial:

***********
R tutorials
***********


Writing Bindings Walkthrough
============================

The first R package tutorial to be included in the New Contributor's
guide is a **Walkthrough** added in the **Writing Bindings**
vignette. With time we will try to include additional tutorials
directly into this guide.

This tutorial will show how to do a binding of a C++ function
`starts_with() <https://arrow.apache.org/docs/cpp/compute.html#containment-tests>`_
to the (base) R function ``startsWith()``.

To view the tutorial follow the
`Walkthrough section of the Writing Bindings article <https://arrow.apache.org/docs/r/articles/developers/bindings.html#walkthrough>`_.


R tutorial on adding a lubridate binding
========================================

In this tutorial, we will document the contribution of a binding
to Arrow R package following the steps specified by the
:ref:`quick-ref-guide` section of the guide and a more detailed
:ref:`step_by_step` section. Navigate there whenever there is
some information you may find is missing here.

The binding will be added to the ``expression.R`` file in the
R package. But you can also follow these steps in case you are
adding a binding that will live somewhere else.

.. seealso::

   To read more about the philosophy behind R bindings, refer to the
   `Writing Bindings article <https://arrow.apache.org/docs/r/articles/developers/bindings.html>`_.

This tutorial is different from the :ref:`step_by_step` as we
will be working on a specific case. This tutorial is not meant
as a step-by-step guide.

**Let's start!**

Set up
------

Let's set up the Arrow repository. We presume here that Git is
already installed. Otherwise please see the :ref:`set-up` section.

Once the `Apache Arrow repository <https://github.com/apache/arrow>`_
is forked (see :ref:`fork-repo-guide`) we will clone it and add the
link of the main repository to our upstream.

.. code:: console

   $ git clone https://github.com/<your username>/arrow.git
   $ cd arrow
   $ git remote add upstream https://github.com/apache/arrow

Building R package
------------------

The steps to follow for for building the R package differs depending on the operating
system you are using. For this reason we will only refer to
the instructions for the building process in this tutorial.

.. seealso::

   For the **introduction** to the building process refer to the
   :ref:`build-arrow-guide` section.

   For the **instructions** on how to build the R package refer to the
   `R developer docs <https://arrow.apache.org/docs/r/articles/developing.html>`_.

The issue
---------

In this tutorial we will be tackling an issue for implementing
a simple binding for ``mday()`` function that will match that of the
existing R function from ``lubridate``.

.. note::

   If you do not have an issue and you need help finding one please refer
   to the :ref:`finding-issues` part of the guide.

Once you have an issue picked out and assigned to yourself, you can
proceed to the next step.

Start the work on a new branch
------------------------------

Before we start working on adding the binding we should
create a new branch from the updated main.

.. code:: console

   $ git checkout main
   $ git fetch upstream
   $ git pull --ff-only upstream main
   $ git checkout -b ARROW-14816

Now we can start with researching the R function and the C++ Arrow
compute function we want to expose or connect to.

**Examine the lubridate mday() function**

Going through the `lubridate documentation <https://lubridate.tidyverse.org/reference/day.html>`_
we can see that ``mday()`` takes a date object
and returns the day of the month as a numeric object.

We can run some examples in the R console to help us understand
the function better:

.. code-block:: R

   > library(lubridate)
   > mday(as.Date("2000-12-31"))
   [1] 31
   > mday(ymd(080306))
   [1] 6

**Examine the Arrow C++ day() function**

From the `compute function documentation <https://arrow.apache.org/docs/cpp/compute.html#containment-tests>`_
we can see that ``day`` is a unary function, which means that it takes
a single data input. The data input must be a ``Temporal class`` and
the returned value is an ``Integer/numeric`` type.

The ``Temporal class`` is specified as: Date types (Date32, Date64),
Time types (Time32, Time64), Timestamp, Duration, Interval.

We can call an Arrow C++ function from an R console using ``call_function``
to see how it works:

.. code-block:: R

   > call_function("day", Scalar$create(lubridate::ymd("2000-12-31")))
   Scalar
   31

We can see that lubridate and Arrow functions operate on and return
equivalent data types. lubridate's ``mday()`` function has no additional
arguments and there are also no option classes associated with Arrow C++
function ``day()``.

.. note::

   To see what to do if there is an option class associated with the
   function you are binding, refer to
   `Examining the C++ function <https://arrow.apache.org/docs/r/articles/developers/bindings.html#examining-the-c-function>`_ from the Writing Bindings
   article.

Looking at the code in ``expressions.R`` we can see the day function
is already specified/mapped on the R package side:
`<https://github.com/apache/arrow/blob/658bec37aa5cbdd53b5e4cdc81b8ba3962e67f11/r/R/expression.R#L63-L64>`_

We only need to add ``mday()`` to the list of expressions connecting
it to the C++ ``day`` function.

.. code-block:: R

   # second is defined in dplyr-functions.R
   # wday is defined in dplyr-functions.R
   "mday" = "day",
   "yday" = "day_of_year",
   "year" = "year",

Adding a test
-------------

Now we need to add a test that checks if everything works well.
If there are additional options or edge cases, we would have to
add more. Looking at tests for similar functions (for example
``yday()`` or ``day())`` we can see that a good place to add two
tests we have is in ``test-dplyr-funcs-datetime.R``:

.. code-block:: R

   test_that("extract mday from timestamp", {
     compare_dplyr_binding(
       .input %>%
         mutate(x = mday(datetime)) %>%
         collect(),
       test_df
     )
   })

And

.. code-block:: R

   test_that("extract mday from date", {
     compare_dplyr_binding(
       .input %>%
         mutate(x = mday(date)) %>%
         collect(),
       test_df
     )
   })

Now we need to see if the tests are passing or we need to do some
more research and code corrections.

.. code-block:: R

   devtools::test(filter="datetime")

   > devtools::test(filter="datetime")
   ℹ Loading arrow
   See arrow_info() for available features
   ℹ Testing arrow
   See arrow_info() for available features
   ✔ | F W S  OK | Context
   ✖ | 1     230 | dplyr-funcs-datetime [1.4s]
   ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
   Failure (test-dplyr-funcs-datetime.R:187:3): strftime
   ``%>%`(...)` did not throw the expected error.
   Backtrace:
    1. testthat::expect_error(...) test-dplyr-funcs-datetime.R:187:2
    2. testthat:::expect_condition_matching(...)
   ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

   ══ Results ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
   Duration: 1.4 s

   [ FAIL 1 | WARN 0 | SKIP 0 | PASS 230 ]

There is a failure we get for the ``strftime`` function but looking
at the code we see is not connected to our work. We can move on and
maybe ask others if they are getting similar fail when running the tests.
It could be we only need to rebuild the library.

Check styling
-------------

We should also run linters to check that the styling of the code
follows the `tidyverse style <https://style.tidyverse.org/>`_. To
do that we run the following command in the shell:

.. code:: console

   $ make style
   R -s -e 'setwd(".."); if (requireNamespace("styler")) styler::style_file(setdiff(system("git diff --name-only | grep r/.*R$", intern = TRUE), file.path("r", source("r/.styler_excludes.R")$value)))'
   Loading required namespace: styler
   Styling  2  files:
    r/R/expression.R                             ✔
    r/tests/testthat/test-dplyr-funcs-datetime.R ℹ
   ────────────────────────────────────────────
   Status   Count Legend
   ✔  1  File unchanged.
   ℹ  1  File changed.
   ✖  0  Styling threw an error.
   ────────────────────────────────────────────
   Please review the changes carefully!


Creating a Pull Request
-----------------------

First let’s review our changes in the shell using ``git status`` to see
which files have been changed and to commit only the ones we are working on.

.. code:: console

   $ git status
   On branch ARROW-14816
   Changes not staged for commit:
     (use "git add <file>..." to update what will be committed)
     (use "git restore <file>..." to discard changes in working directory)
      modified:   R/expression.R
      modified:   tests/testthat/test-dplyr-funcs-datetime.R

And ``git diff`` to see the changes in the files in order to spot any error we might have made.

.. code:: console

   $ git diff
   diff --git a/r/R/expression.R b/r/R/expression.R
   index 37fc21c25..0e71803ec 100644
   --- a/r/R/expression.R
   +++ b/r/R/expression.R
   @@ -70,6 +70,7 @@
      "quarter" = "quarter",
      # second is defined in dplyr-functions.R
      # wday is defined in dplyr-functions.R
   +  "mday" = "day",
      "yday" = "day_of_year",
      "year" = "year",

   diff --git a/r/tests/testthat/test-dplyr-funcs-datetime.R b/r/tests/testthat/test-dplyr-funcs-datetime.R
   index 359a5403a..228eca56a 100644
   --- a/r/tests/testthat/test-dplyr-funcs-datetime.R
   +++ b/r/tests/testthat/test-dplyr-funcs-datetime.R
   @@ -444,6 +444,15 @@ test_that("extract wday from timestamp", {
      )
    })

   +test_that("extract mday from timestamp", {
   +  compare_dplyr_binding(
   +    .input %>%
   +      mutate(x = mday(datetime)) %>%
   +      collect(),
   +    test_df
   +  )
   +})
   +
    test_that("extract yday from timestamp", {
      compare_dplyr_binding(
        .input %>%
   @@ -626,6 +635,15 @@ test_that("extract wday from date", {
      )
    })

   +test_that("extract mday from date", {
   +  compare_dplyr_binding(
   +    .input %>%
   +      mutate(x = mday(date)) %>%
   +      collect(),
   +    test_df
   +  )
   +})
   +
    test_that("extract yday from date", {
      compare_dplyr_binding(
        .input %>%

Everything looks OK. Now we can make the commit
(save our changes to the branch history):

.. code:: console

   $ git commit -am "Adding a binding and a test for mday() lubridate"
   [ARROW-14816 ed37d3a3b] Adding a binding and a test for mday() lubridate
    2 files changed, 19 insertions(+)

We can use ``git log`` to check the history of commits:

.. code:: console

   $ git log
   commit ed37d3a3b3eef76b696532f10562fea85f809fab (HEAD -> ARROW-14816)
   Author: Alenka Frim <frim.alenka@gmail.com>
   Date:   Fri Jan 21 09:15:31 2022 +0100

       Adding a binding and a test for mday() lubridate

   commit c5358787ee8f7b80f067292f49e5f032854041b9 (upstream/main, upstream/HEAD, main, ARROW-15346, ARROW-10643)
   Author: Krisztián Szűcs <szucs.krisztian@gmail.com>
   Date:   Thu Jan 20 09:45:59 2022 +0900

       ARROW-15372: [C++][Gandiva] Gandiva now depends on boost/crc.hpp which is missing from the trimmed boost archive

       See build error https://github.com/ursacomputing/crossbow/runs/4871392838?check_suite_focus=true#step:5:11762

       Closes #12190 from kszucs/ARROW-15372

       Authored-by: Krisztián Szűcs <szucs.krisztian@gmail.com>
       Signed-off-by: Sutou Kouhei <kou@clear-code.com>

If we started the branch some time ago, we may need to rebase
to upstream main to make sure there are no merge conflicts:

.. code:: console

   $ git pull upstream main --rebase

And now we can push our work to the forked Arrow repository
on GitHub called origin.

.. code:: console

   $ git push origin ARROW-14816
   Enumerating objects: 233, done.
   Counting objects: 100% (233/233), done.
   Delta compression using up to 8 threads
   Compressing objects: 100% (130/130), done.
   Writing objects: 100% (151/151), 35.78 KiB | 8.95 MiB/s, done.
   Total 151 (delta 129), reused 33 (delta 20), pack-reused 0
   remote: Resolving deltas: 100% (129/129), completed with 80 local objects.
   remote:
   remote: Create a pull request for 'ARROW-14816' on GitHub by visiting:
   remote:      https://github.com/AlenkaF/arrow/pull/new/ARROW-14816
   remote:
   To https://github.com/AlenkaF/arrow.git
    * [new branch]          ARROW-14816 -> ARROW-14816

Now we have to go to the `Arrow repository on GitHub <https://github.com/apache/arrow>`_
to create a Pull Request. On the GitHub Arrow
page (main or forked) we will see a yellow notice
bar with a note that we made recent pushes to the branch
ARROW-14816. That’s great, now we can make the Pull Request
by clicking on **Compare & pull request**.

.. figure:: /developers/images/R_tutorial_create_pr_notice.jpeg
   :scale: 60 %
   :alt: GitHub page of the Apache Arrow repository showing a notice bar
         indicating change has been made in our branch and a Pull Request
         can be created.

   Notice bar on the Apache Arrow repository.

First we need to change the Title to **ARROW-14816: [R] Implement
bindings for lubridate::mday()** in order to match it with the
issue. Note a punctuation mark was added!

*Extra note: when this tutorial was created, we had been using the Jira issue
tracker. As we are currently using GitHub issues, the title would be prefixed
with GH-14816: [R] Implement bindings for lubridate::mday()*.

We will also add a description to make it clear to others what we are trying to do.

.. figure:: /developers/images/R_tutorial_pr_descr.jpeg
   :scale: 50 %
   :alt: GitHub page of the Pull Request showing the editor for the
         title and a description.

   Editing the title and the description of our Pull Request.

Once we click **Create pull request** our code can be reviewed as
a Pull Request in the Apache Arrow repository.

.. figure:: /developers/images/R_tutorial_pr.jpeg
   :scale: 50 %
   :alt: GitHub page of the Pull Request showing the title and a
         description.

   Here it is, our Pull Request!

The pull request gets connected to the issue and the CI is running.
After some time passes and we get a review we can correct the code,
comment, resolve conversations and so on.

.. seealso::

   For more information about Pull Request workflow see :ref:`pr_lifecycle`.

The Pull Request we made can be viewed `here <https://github.com/apache/arrow/pull/12218>`_.
