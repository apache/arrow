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

.. _introduction:

***********************
New Contributor's Guide
***********************

This guide is meant to be a resource for contributing to
`Arrow <https://arrow.apache.org//>`_ for new contributors.

Starting to contribute to a project like Apache Arrow can look
scary if not completely impossible. But we can reassure you it is
not. What we have to do is to take small steps.


Why contribute to Arrow?
========================
There can be various reasons why someone would want to contribute
to Arrow and all of them are valid!

* You find the project interesting and would like to give it a go,
  learn and grow.

* You use Arrow in the project you are working on and it would be
  great to make some quick fixes by yourself

Read more about the project in the :ref:`about_arrow` section.

Quick Reference
===============

Here are the basic steps needed to get set up and contribute to Arrow.
This is meant as a checklist and also to have an overall picture.

For complete instructions please follow :ref:`step_by_step` - a
step-by-step guide or R and Python :ref:`tutorial-index` for an example
of adding a basic feature.


#. **Install and set up Git, fork the Arrow repository**

   See detailed instructions on how to :ref:`set-up` Git and fork the
   Arrow repository.

#. **Building Arrow**

   This can be quite easy or a real blocker. Be prepared to dig through the
   additional flags for cmake in the C++ build guide and read the doc carefully
   to not miss any suggestions already given there. Anytime you are stuck,
   reach out to us via appropriate :ref:`communication` channel.

   See detailed instructions on how to :ref:`build-arrow`.
 
#. **Run the tests**
   
   Run the tests from a terminal for Python

   .. code-block::

      $ pytest pyarrow

   or R

   .. code-block::

      $ devtools::test()

#. **Find an issue, create a new branch and work on the problem**

   **FInding an issue**

   Not so easy, we agree.
   Read through the :ref:`finding-issues` section to get some ideas.

   **Finding your way through the project**

   You have an issue, you build Arrow and now you are completely lost. We get you.
   It is a complicated project and the first step is the hardest. We wrote some
   help guides that we used when we started. You will see that at the end it will
   all look much simpler as it did at the start.

   Read through :ref:`solving` section.

   **Communication**

   Communication is very important. You may ned some help solving the problem
   (happening to developers all the time). Also, when you have an issue you want
   to solve it is advisable to let the team know you are working on it and may
   need some help.

   See possible channels of :ref:`communication`.

#. **Once you found the solution, write and run the test**

   See detailed instructions on how to do the :ref:`testing`.

#. **Push the branch on your fork and create a pull request**

   See detailed instructions on :ref:`pr_and_github`


If you are ready you can start with building Arrow or choose to follow tutorials
on writing an R or Python test.

**We want to encourage everyone to contribute to Arrow!**


Full Table of Contents
======================

.. toctree::
   :maxdepth: 3

   about_arrow
   communication
   step_by_step/index
   documentation
   tutorials/index
   other