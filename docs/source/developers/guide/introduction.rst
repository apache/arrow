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
.. Make an introduction to the Guide for New Contributors.
.. Add an introduction to the project, why to get involved
.. and a quick checklist of the contributing process.
.. Add the Table of Contents. All detailed explanations
.. should be added as a link in the content.


.. _introduction:

***********************
New Contributor's Guide
***********************

This guide is meant to be a resource for contributing to
`Arrow <https://arrow.apache.org>`_ for new contributors.

No matter what your current skills are you can try and make
your first time contribution to Arrow.

Starting to contribute to a project like Apache Arrow can be
intimidating. Taking small steps will help making this tack
easier.


Why contribute to Arrow?
========================
There can be various reasons why someone would want to contribute
to Arrow:

* You find the project interesting and would like to try making
  a contribution to learn more about the library and grow your skills.

* You use Arrow in the project you are working on and you would like
  to implement a new feature.

Read more about the project in the :ref:`architectural_overview` section.

Quick Reference
===============

Here are the basic steps needed to get set up and contribute to Arrow.
This is meant as a checklist and also to have an overall picture.

For complete instructions please follow :ref:`step_by_step` (a
step-by-step guide) or R and Python :ref:`tutorial-index` for an example
of adding a basic feature.


#. **Install and set up Git, fork the Arrow repository**

   See detailed instructions on how to :ref:`set-up` Git and fork the
   Arrow repository.

#. **Build Arrow**

   Arrow libraries include a wide range of functionalities and may require
   the installation of third-party packages, depending on which build
   options and components you enable. The C++ build guide
   has suggestions for for commonly encountered issues - you can find it
   `here <https://arrow.apache.org/docs/developers/cpp/index.html#cpp-development>`_. 
   Anytime you are stuck, feel free to reach out via
   appropriate :ref:`communication` channel.

   See a short description about the building process of 
   :ref:`PyArrow or R-Arrow<build-arrow>` or go straight to detailed
   instructions on how to build one of Arrow libraries in the
   `documentation <https://arrow.apache.org/docs/>`_ .
 
#. **Run the tests**
   
   Run the tests from a terminal for Python

   .. code-block::

      $ pytest pyarrow

   or in R console for R

   .. code-block:: R

      devtools::test()

   See also the section on :ref:`testing`.

#. **Find an issue (if needed), create a new branch and work on the problem**

   **Finding an issue**

   You can have a new bug or a new feature you want to implement. But if you
   don't and you need an issue to work on you may need help finding it. Read
   through the :ref:`finding-issues`  section to get some ideas.

   **Finding your way through the project**

   The first step when starting a new project is the hardest. We wrote some
   help guides that we used when we were looking for solutions and we hope
   they will help.

   .. TODO: Read through :ref:`solving`  section.

   **Communication**

   Communication is very important. You may need some help solving problems
   you encounter on the way (happening to developers all the time). Also,
   if you have a JIRA issue you want to solve it is advisable to let the team
   know you are working on it and may need some help.

   See possible channels of :ref:`communication`.

#. **Once you implemented the planned fix or feature, write and run tests for it**

   See detailed instructions on how to :ref:`test <testing>`.

#. **Push the branch on your fork and create a Pull Request**

   See detailed instructions on :ref:`pr_and_github`


If you are ready you can start with building Arrow or choose to follow tutorials
on writing an R or Python test.

**We want to encourage everyone to contribute to Arrow!**


Full Table of Contents
======================

.. toctree::
   :maxdepth: 3

   architectural_overview
   communication
   step_by_step/index
   documentation
   tutorials/index
   other