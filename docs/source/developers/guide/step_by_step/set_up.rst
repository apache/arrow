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
.. This section should include all necessary steps to set up
.. the forked Arrow repository locally in order to be able to
.. proceed toward building the library.


.. _set-up:

******
Set up
******

Install and set up Git
======================

The Arrow project is developed using `Git <https://git-scm.com/>`_
for version control which is easily available for all common
operating systems.

You can follow the instructions to install Git from GitHub
where Arrow repository is hosted, following
`the quickstart instructions <https://docs.github.com/en/get-started/quickstart/set-up-git>`_.

When Git is set up do not forget to configure your name and email

.. code:: console

   $ git config --global user.name "Your Name"
   $ git config --global user.email your.email@example.com

and `authenticate with GitHub <https://docs.github.com/en/get-started/quickstart/set-up-git#next-steps-authenticating-with-github-from-git>`_
as this will allow you to interact with GitHub without typing
a username and password each time you execute a git command.

.. note::

   This guide assumes you are comfortable working from the command line.
   Some IDEs allow you to manage a Git repository, but may implicitly run
   unwanted operations when doing so (such as creating project files).

   For example, cloning it in RStudio assumes the whole repository is an
   RStudio project and will create a ``.Rproj`` file in the root directory.
   For this reason it is *highly recommended* to clone the repository using
   the command line or a Git client.
   
Get the source code
===================

.. _fork-repo-guide:

Fork the repository
-------------------

The Arrow GitHub repository contains both the Arrow C++ library plus
libraries for other languages such as Go, Java, Matlab, Python, R, etc.
The first step to contributing is to create a fork of the repository
in your own GitHub account.

1. Go to `<https://github.com/apache/arrow>`_.

2. Press Fork in the top right corner.

   .. figure:: ../../images/github_fork.jpeg
      :scale: 50 %
      :alt: Fork the Apache Arrow repository on GitHub.

      The icon to fork the Apache Arrow repository on GitHub.

3. Choose to fork the repository to your username so the fork will be
   created at ``https://github.com/<your username>/arrow``.

Clone the repository
--------------------

Next you need to clone the repository

.. code:: console

   $ git clone https://github.com/<your username>/arrow.git

and add Apache Arrow repository as a remote called ``upstream``.

.. code:: console

   $ cd arrow
   $ git remote add upstream https://github.com/apache/arrow

Verify your upstream
--------------------

Your upstream should be pointing at the Arrow GitHub repo.

Running in the shell:

.. code:: console

   $ git remote -v

Should give you a result similar to this:

.. code:: console

   origin	https://github.com/<your username>/arrow.git (fetch)
   origin	https://github.com/<your username>/arrow.git (push)
   upstream	https://github.com/apache/arrow (fetch)
   upstream	https://github.com/apache/arrow (push)

If you did everything correctly, you should now have a copy of the code
in the ``arrow`` directory and two remotes that refer to your own GitHub
fork (``origin``) and the official Arrow repository (``upstream``).
