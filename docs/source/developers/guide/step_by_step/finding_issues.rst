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
.. about JIRA, how to find issues or how to create one.
.. Should not duplicate with Report bugs and propose features
.. section:

..   https://arrow.apache.org/docs/developers/contributing.html#tips-for-using-jira


.. _finding-issues:

****************************
Finding good first issues 🔎
****************************

You have successfully built Arrow library, congrats!

The next step is finding something to work on. As was mentioned before,
you might already have a bug to fix in mind, or a new feature that you want to implement.
Or you still need an issue to work on and you need some help with finding one.

For both cases JIRA is the issue tracker we use.

First we will explain how to use JIRA if you have a fix or a feature to work on but
there is no issue about it created. In that case you need to create an issue yourself.

Secondly we will show a way to find good first issues.

How to create a JIRA issue
==========================

First thing you need to do is to make an account on the ASF JIRA following
`this link <https://issues.apache.org/jira/secure/Signup!default.jspa>`_. You will have to
select your language and choose an avatar. If the registration is successful you will see:

.. figure:: jira_1.jpeg
   :scale: 70 %
   :alt: creating an ASF JIRA account

   The window you get after creating an account on the ASF JIRA.

Here you can click **Create an issue** and select **Apache Arrow project** and **Type** of
the issue (Bug, Feature, …).

.. figure:: jira_2.jpeg
   :scale: 70 %
   :alt: creating JIRA issue

   The window to create a JIRA issue.

You are ready to create the issue! Add a title and a description following the :ref:`tips for using JIRA <jira-tips> and you are ready to go!

.. seealso::
	:ref:`Tips for using JIRA <jira-tips>`

You don’t need any special permissions on JIRA to be able to create issues.
Once you are more involved in the project and want to do more on JIRA, for example assigning
yourself an issue, you will need “Contributor” permissions. To get this role, ask on the
mailing list or in the comment of the JIRA issue you created.

When the ticket is created you can start a discussion about it in the JIRA comments section.

How we use JIRA to find an issue
================================

To make it easier to find issues that are suited for a new contributor we added labels
like “good-first-issue” or “beginner”.

.. seealso::
	Search for good first/second issues with labels like in the `link here
	<https://issues.apache.org/jira/browse/ARROW-14659?filter=-4&jql=project%20%3D%20ARROW%20AND%20status%20%3D%20Open%20AND%20labels%20in%20(Beginner%2C%20beginner%2C%20beginners%2C%20beginnner%2C%20beginner-friendly%2C%20good-first-issue%2C%20good-second-issue%2C%20GoodForNewContributors%2C%20newbie%2C%20easyfix%2C%20documentation)%20order%20by%20created%20DESC>`_

The issues labeled as good first issues should take no more than two days or a weekend to
fix them. Once you dig into the code you may find that the issue is not easy at all - it can
happen as the label is set when making a triage and the problem could be harder then expected.
Don't hesitate to write that in the comments.

.. figure:: jira_3.jpeg
   :scale: 45 %
   :alt: finding good first JIRA issue

   Example of the list of good first issues.

.. note::
	When you find a JIRA issue you would work on, please mention your interest in the comment
	section of that issue. That way we will know you are working on it.

Also do not hesitate to ask questions in the comment section of the issue.
You can get some pointers about where to start and similar issues already solved.

**Do not forget to create a new branch once you created or chose an issue you will be
working on!** Follow the instructions in the next section: :ref:`pr_and_github`
