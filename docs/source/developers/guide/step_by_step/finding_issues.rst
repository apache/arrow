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
.. This section should include additional information
.. about GitHub, how to find issues or how to create one.
.. Should not duplicate with Report bugs and propose features
.. section:

..   https://arrow.apache.org/docs/developers/bug_reports.html#bug-reports


.. _finding-issues:

****************************
Finding good first issues 🔎
****************************

You have successfully built the Arrow library; congrats!

The next step is finding something to work on. As mentioned before,
you might already have a bug to fix in mind, or a new feature that
you want to implement. Or you still need an issue to work on and
you need some help with finding one.

For both cases, GitHub is the issue tracker that we use.

- If you do not have a GitHub account yet, navigate to the
  `GitHub login page <https://github.com/join>`_ to create one.
- If you need help with creating a new GitHub issue see the
  `GitHub documentation <https://docs.github.com/en/issues/tracking-your-work-with-issues/creating-an-issue>`_.

When the ticket is created you can start a discussion about it in the GitHub comments.

GitHub labels
=============

To make it easier for you to find issues that are well-suited for new
contributors, we have added a label **“good-first-issue”** to some
GitHub issues.

.. seealso::
   Search for good first issues `good-first-issue label listing
   <https://github.com/apache/arrow/labels/good-first-issue>`_

The issues labeled as good first issues should take no more than two days or
a weekend to fix them. Once you dig into the code you may find that the issue
is not easy at all - this can happen as the problem could be harder than the
person who triaged the ticket expected it to be. Don't hesitate to write that
in the comments.

.. note::
   
   When you find a GitHub issue you would like to work on, please mention
   your interest in the comment section of that issue; that way we will know
   you are working on it.

Also, do not hesitate to ask questions in the comment. You can get some
pointers about where to start and similar issues already solved.

**What if an issue is already asigned?**
When in doubt, comment on the issue asking if they mind if you try to put
together a pull request; interpret no response to mean that you’re free to
proceed.

**Ask questions**
Please do ask questions, either on the GitHub issue itself or on the dev
mailing list, if you have doubts about where to begin or what approach to
take. This is particularly a good idea if this is your first code contribution,
so you can get some sense of what the core developers in this part of the
project think a good solution looks like. For best results, ask specific,
direct questions, such as:

* Do you think $PROPOSED_APPROACH is the right one?
* In which file(s) should I be looking to make changes?
* Is there anything related in the codebase I can look at to learn?

If you ask these questions and do not get an answer, it is OK to ask again.

.. note::

   **Do not forget to create a new branch once you have created or chosen an
   issue you will be working on!** Follow the instructions in the
   :ref:`pr_lifecycle` section or follow the next section: :ref:`arrow-codebase`.
