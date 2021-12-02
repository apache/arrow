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
.. This section should include all steps in making a pull
.. request (until it is merged) on Arrow GitHub repository
.. using git.


.. _pr_and_github:

******************************
Lifecycle of a Pull Request 🙀 
******************************

:ref:`As mentioned before<set-up>`, Arrow project uses git for
version control and a workflow based on Pull Requests. That means
that you contribute the changes to the code by creating a branch
in Git, make changes to the code, push the changes to you ``origin``
which is your fork of the Arrow repository on GitHub and then you
create a **Pull Request** against the official Arrow repository
which is saved in your set up as ``upstream``.

You should have git set up by now, have your source code,
have successfully built Arrow and have an JIRA issue to work on.

**Before making changes to the code, you should create a new
branch in Git.**

1. Update/sync the code from your ``upstream``
   in the master branch. Run it in the shell from ``arrow`` directory.

   .. code-block:: shell

      $ git checkout master
      $ git fetch upstream
      $ git pull --ff-only upstream master

2. Create a new branch

   .. code-block:: shell

      $ git checkout -b <branch-name>

Now you can make changes to the code. To see the changes
made in the library use this two commands:

.. code-block:: shell

   $ git status # to see what files are changed
   $ git diff # to see code change per file

Creating a Pull Request 
=======================

Once you are satisfied with the changes, run the :ref:`tests <testing>`
and linters and then go ahead and commit the changes.

3. Add and commit the changes

   .. code-block:: shell
         
      $ git add <filenames>
      $ git commit -m '<message>'

   Or you can add and commit in one step, if all the files changed
   are to be committed
   
   .. code-block:: shell      

      $ git commit -a -m '<message>'

4. Then push your work to your Arrow fork

   .. code-block:: shell  

      $ git push origin <branch-name>

.. note::

   Your work is now still under your watchful eye so no problem if you
   see any error you would like to correct. You can make an additional
   commit to correct. Also Git has lots of ways to
   amend, delete, revise, etc. See https://git-scm.com/docs for more
   information.

   Until you make the Pull Request, nothing is visible on the Arrow
   repository and you are free to experiment.

If all is set, you can make the Pull Request!

5. Go to ``https://github.com/YOU/arrow`` where you will see a box with
   the name of the branch that you pushed and next to it a green button
   **Compare & Pull Request**. Clicking on it you should add a title and
   description of the Pull Request. Underneath you can check once again
   the changes you have made.

   .. seealso::
      
      Get more details on naming the Pull Request in Arrow repository
      and other additional information :ref:`pull_request_and_review`
      section.

Reviews and merge of the Pull Request
=====================================

When the Pull Request is submitted it waits to get reviewed. One of
great things about Open Source is your work gets lots of feedback and
so it gets perfected. Do not be discouraged by the time it takes for
the PR to get merged due to reviews and corrections. It is a process
that supports quality and with it you can learn a lot.

If it still takes too long to get merged, do not hesitate to remind
maintainers in the comment section of the Pull Request and post
reminders on the JIRA ticket also.

How to get your Pull Request to be reviewed?
--------------------------------------------

Arrow maintainers will be notified when a Pull Request is created and
they will get to it as soon as possible. If days pass and it still had
not been reviewed go ahead and mention the reporter of the JIRA issue 
or a developer that you communicated with via JIRA comments, mailing
list or GitHub.

To put a **mention** in GitHub insert @ in the comment and select the
username from the list.

Commenting on a Pull Request
----------------------------

When a Pull Request is open in the repository you and other developers
can comment on the proposed solution.

To create a general comment navigate to the **Conversation** tab of
you Pull Request and start writing in the comment box at the bottom of
the page.

You can also comment on a section of the file to point out something
specific from your code. To do this navigate to **Files changed** tab and
select a line you want to insert the comment to. Hovering over the beginning
of the line you will see a **blue plus icon**. You can click on it or drag
it to select multiple lines and then click the icon to insert the comment.

Resolve conversation
--------------------

You can resolve a conversion in a Pull Request review by clicking
**Resolve conversation** in the **Files changed** tab. This way the
conversation will be collapsed and marked as resolved which will make it
easier for you to organize what is done and what still needs to be addressed.

After getting a review
----------------------

The procedure after getting reviews is similar to creating the initial Pull Request.
You need to update your code locally, make a commit, update the branch to sync
it with upstream (or origin if there were commits from other developers or if you 
committed suggestions from the GitHub) and push your code to origin. It will
automatically be updated in you Pull Request also.

.. seealso::

   See more about updating the branch (we use ``rebase``, not ``merge``) in
   the review process :ref:`here <git_conventions>`. 

If the review process is successful your Pull Request will get merged.

Congratulations! 🎉
===================