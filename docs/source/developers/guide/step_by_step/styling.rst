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

.. _styling:

**********
Styling 😎
**********

Each language in the Apache Arrow project follows its own style guides.

In this section we will provide links to the existing documentation
to make it easier for you to find the relevant information about
linters and styling of the code.

.. tabs::

   .. tab:: PyArrow

      We use flake8 linting for styling issues in Python. To help
      developers check styling of the code, among other common
      development tasks, :ref:`Archery utility<archery>` tool was
      developed within Apache Arrow.

      The instructions on how to set up and use Archery
      can be found in the Coding Style section of the
      :ref:`python-development`.

   .. tab:: R package

      For the R package you can use ``{lintr}`` or ``{styler}``
      to check if the code follows the
      `tidyverse style <https://style.tidyverse.org/>`_.

      The instructions on how to use either of these two packages
      can be found in the
      `Styling and Linting section of the Common developer workflow tasks <https://arrow.apache.org/docs/r/articles/developers/workflow.html#styling-and-linting>`_.

Pre-commit
----------

It is useful to set up `pre-commit <https://pre-commit.com/>`_,
a multi-language package manager for pre-commit hooks. It will
check your code and will stop the commit process, described in
the following section, if there are any errors.

- `Pre-commit installation instructions <https://pre-commit.com/#installation>`_
- `Pre-commit hooks <https://pre-commit.com/hooks.html>`_