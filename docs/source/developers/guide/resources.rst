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
.. Add articles/resources on concepts important to Arrow as
.. well as recommended books for learning different languages
.. included in the project.


.. _other-resources:

************************************
Additional information and resources
************************************

On this page we have listed resources that may be relevant or useful for
contributors who want to learn more about different parts of Apache Arrow.

Glossary
--------
List of common terms in Apache Arrow project with a short description can
be found in :doc:`the glossary <../../format/Glossary>`.

Additional information
----------------------

- GitHub Actions

  GitHub Actions is a continuous integration and continuous delivery (CI/CD) platform.
  Apache Arrow has a set of workflows that run via GitHub Actions to build and test
  every pull request that is opened, merged, etc.

  - `Apache Arrow Actions on GitHub <https://github.com/apache/arrow/actions>`_
  - `Location of the workflows in Arrow: arrow/.github/workflows/ <https://github.com/apache/arrow/tree/main/.github/workflows>`_
  - `GitHub Documentation on GitHub Actions <https://docs.github.com/en/actions>`_

  .. ARROW-13841: [Doc] Document the different subcomponents that make up the CI and how they fit together:
  .. https://github.com/apache/arrow/pull/11821

- Nightly builds

  It is possible to install the Arrow R package from the nightly builds which are daily development
  builds of the R package and are not the official releases. See more on the
  `Install R package article <https://arrow.apache.org/docs/dev/r/articles/install.html#install-the-nightly-build>`_.

- `Apache Arrow releases <https://arrow.apache.org/release/>`_

Other resources
---------------
Github

- `GitHub docs: Fork a repo <https://docs.github.com/en/get-started/quickstart/fork-a-repo>`_
- `GitHub: Creating a pull request from a fork <https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork>`_

Contributing

- :ref:`contributing`
- `Arrow R Developer Guide <https://arrow.apache.org/docs/r/articles/developing.html>`_
- `Writing Bindings article for R package <https://arrow.apache.org/docs/r/articles/developers/bindings.html>`_.

Reproducible examples:

- `Tidyverse: Make a reprex <https://www.tidyverse.org/help/#reprex>`_
- `Craft Minimal Bug Reports by Matthew Rocklin <https://matthewrocklin.com/blog/work/2018/02/28/minimal-bug-reports>`_

Recommended references 
----------------------

- Slatkin, Brett, *Effective Python: 90 Specific Ways to Write Better Python*, Addison-Wesley Professional, 2019
- Stroustrup, Bjarne, *A Tour of C++ (Second edition)*, Addison-Wesley, 2018
- Wickham, Hadley, `R Packages: Organize, Test, Document, and Share Your Code <https://r-pkgs.org/>`_
- Wickham, Hadley, `Advanced R <https://adv-r.hadley.nz/>`_
