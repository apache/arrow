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

.. highlight:: console

.. _contributing:

****************************
Contributing to Apache Arrow
****************************

**Thanks for your interest in the Apache Arrow project.**

Arrow is a large project and may seem overwhelming when you're
first getting involved. Contributing code is great, but that's
probably not the first place to start. There are lots of ways to
make valuable contributions to the project and community.

This page provides some orientation for how to get involved. It also offers
some recommendations on how to get the best results when engaging with the
community.

Code of Conduct
===============

All participation in the Apache Arrow project is governed by the ASF's
`Code of Conduct <https://www.apache.org/foundation/policies/conduct.html>`_.

.. grid:: 2
   :gutter: 4
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: Community
      :img-top: /developers/images/users-solid.svg
      :class-card: contrib-card
      :shadow: md

      A good first step to getting involved in the Arrow project is to join
      the mailing lists and participate in discussions where you can.

      +++

      .. button-link:: https://arrow.apache.org/community/
         :click-parent:
         :color: secondary
         :expand:

         Apache Arrow Community

   .. grid-item-card:: Bug reports
      :img-top: /developers/images/bug-solid.svg
      :class-card: contrib-card
      :shadow: md

      Alerting us to unexpected behavior and missing features, even
      if you can't solve the problems yourself, help us understand
      and prioritize work to improve the libraries.

      +++

      .. button-ref:: bug-reports
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         Bugs and Features

.. dropdown:: Communicating through the mailing lists
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   Projects in The Apache Software Foundation ("the ASF") use public, archived
   mailing lists to create a public record of each project's development
   activities and decision-making process.

   While lacking the immediacy of chat or other forms of communication,
   the mailing lists give participants the opportunity to slow down and be
   thoughtful in their responses, and they help developers who are spread across
   many timezones to participate more equally.

   Read more on the `Apache Arrow Community <https://arrow.apache.org/community/>`_
   page.

.. dropdown:: Improve documentation
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md

   A great way to contribute to the project is to improve documentation. If you
   found some docs to be incomplete or inaccurate, share your hard-earned knowledge
   with the rest of the community.

   Documentation improvements are also a great way to gain some experience with
   our submission and review process, discussed below, without requiring a lot
   of local development environment setup. In fact, many documentation-only changes
   can be made directly in the GitHub web interface by clicking the "edit" button.
   This will handle making a fork and a pull request for you.

   * :ref:`documentation`
   * :ref:`building-docs`

.. grid:: 2
   :gutter: 4
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: New Contributors
      :img-top: /developers/images/book-open-solid.svg
      :class-card: contrib-card
      :shadow: md

      First time contributing?

      The New Contributor's Guide provides necessary information for
      contributing to the Apache Arrow project.

      +++

      .. button-ref:: guide-introduction
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         New Contributor's guide

   .. grid-item-card:: Overview
      :img-top: /developers/images/code-solid.svg
      :class-card: contrib-card
      :shadow: md

      A short overview of the contributing process we follow
      and some additional information you might need if you are not
      new to the contributing process in general.
      +++

      .. button-ref:: contrib-overview
         :ref-type: ref
         :click-parent:
         :color: secondary
         :expand:

         Contributing overview

Language specific
=================

Connection to the specific language development pages:

.. tab-set::

   .. tab-item:: C++

      * :ref:`cpp-development`
      * :ref:`C++ Development Guidelines <development-cpp>`
      * :ref:`building-arrow-cpp`

   .. tab-item:: Java

      * :doc:`java/index`

   .. tab-item:: Python

      * :ref:`python-development`

   .. tab-item:: R

      * `Arrow R Package: Developer environment setup <https://arrow.apache.org/docs/dev/r/articles/developers/setup.html>`_
      * `Arrow R Package: Common developer workflow tasks <https://arrow.apache.org/docs/dev/r/articles/developers/workflow.html>`_

   .. tab-item:: Ruby

      * `Red Arrow - Apache Arrow Ruby <https://github.com/apache/arrow/tree/main/ruby/red-arrow#development>`_
