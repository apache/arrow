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


.. _guide-introduction:

***********************
New Contributor's Guide
***********************

This guide is a resource for contributing to
Apache Arrow for new contributors.

No matter what your current skills are, you can try and make
your first time contribution to Arrow.

Starting to contribute to a project like Apache Arrow can be
intimidating. Taking small steps will make this task
easier.


Why contribute to Arrow?
========================
There can be various reasons why you might want to contribute
to Arrow:

* You find the project interesting and would like to try making
  a contribution to learn more about the library and grow your skills.

* You use Arrow in a project you are working on and you would like
  to implement a new feature or fix a bug you encountered.

Read more about the project in the :ref:`architectural_overview` section.

.. note::

 Contributors at Apache Arrow are following ASF's
 `Code of Conduct <https://www.apache.org/foundation/policies/conduct.html>`_.

.. _quick-ref-guide:

Quick Reference
===============

Here are the basic steps needed to get set up and contribute to Arrow.
This is meant both as a checklist and also to provide an overall picture of the process.

For complete instructions please follow :ref:`step_by_step` (a
step-by-step guide) or R and Python :ref:`tutorial-index` for an example
of adding a basic feature.


#. **Install and set up Git, and fork the Arrow repository**

   See detailed instructions on how to :ref:`set-up` Git and fork the
   Arrow repository.

#. **Build Arrow**

   Arrow libraries include a wide range of functionalities and may require
   the installation of third-party packages, depending on which build
   options and components you enable. The C++ build guide
   has suggestions for commonly encountered issues - you can find it
   :ref:`here <cpp-development>`. 
   Anytime you are stuck, feel free to reach out via
   appropriate :ref:`communication` channel.

   See a short description about the building process of 
   :ref:`PyArrow or the R package<build-arrow-guide>` or go straight to detailed
   instructions on how to build one of Arrow libraries in the
   `documentation <https://arrow.apache.org/docs/>`_ .
 
#. **Run the tests**
   
   We should run the tests to check if everything is working correctly. For example,
   you can run the tests from a terminal for Python

   .. code-block:: console

      $ pytest pyarrow

   or in R console for R

   .. code-block:: R

      devtools::test()

   See also the section on :ref:`testing`.

#. **Find an issue (if needed), create a new branch and work on the problem**

   **Finding an issue**

   You might already have a bug to fix in mind, or a new feature that you want to
   implement. But if you don't and you need an issue to work on, then you may need
   help finding it. Read through the :ref:`finding-issues`  section to get some ideas.

   **Finding your way through the project**

   The first step when starting a new project is the hardest and so we've
   wrote some guides to help you with this.

   You can start by reading through :ref:`arrow-codebase` section.

   **Communication**

   Communication is very important. You may need some help solving problems
   you encounter on the way (this happens to developers all the time). Also,
   if you have a GitHub issue you want to solve, then it is advisable to let the team
   know you are working on it and may need some help.

   See possible channels of :ref:`communication`.

#. **Once you implemented the planned fix or feature, write and run tests for it**

   See detailed instructions on how to :ref:`test <testing>`. Also run the linter
   to make sure the code is :ref:`styled <styling>` correctly before proceeding
   to the next step!

#. **Push the branch on your fork and create a Pull Request**

   See detailed instructions on :ref:`create_pr`


If you are ready you can start with building Arrow or choose to follow
one of the :ref:`tutorial-index` on writing an R binding or Python feature.

Different ways to contribute
============================

There are lots of ways to contribute to the project besides writing code!

* Improving the **documentation** is a great way to start contributing!
  For more information visit :ref:`documentation` section of the guide.

* **Apache Arrow Cookbooks** are a collection of recipes for solving various problems 
  and completing different tasks using Apache Arrow. They are also a great way to start
  contributing. For more information visit
  `How to contribute to Apache Arrow Cookbook <https://github.com/apache/arrow-cookbook/blob/main/CONTRIBUTING.md>`_
  located in the Apache Arrow Cookbook repository.

You are also welcome to take a look at :ref:`other-resources` section.

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
   resources
