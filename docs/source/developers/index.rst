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

Developers documentation
========================

Welcome to the developers documentation!

.. grid:: 2
   :gutter: 4
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: Contributing
      :class-card: contrib-card
      :shadow: md

      Want to contribute to Apache Arrow?
      Contributions can be in a form of a
      bug report, typo correction in the
      documentation, new feature in the
      codebase etc.

      +++

      .. button-link:: contributing.html
         :click-parent:
         :color: secondary
         :expand:

         Contributing to Apache Arrow

   .. grid-item-card:: Documentation
      :class-card: contrib-card
      :shadow: md

      Are you thinking of making a bigger
      contribution to the documentation and need
      information on how to build the docs
      locally?

      +++

      .. button-link:: documentation.html
         :click-parent:
         :color: secondary
         :expand:

         Building the Documentation

.. dropdown:: Development guidelines and source build instructions
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md
   :open:

   * :doc:`cpp/index`
   * :doc:`java/index`
   * :doc:`python`

.. grid:: 2
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: Development tools
      :class-card: contrib-card
      :shadow: md

      Find the information about GitHub workflows, Archery development
      tool, Docker builds and Crossbow packaging
      +++

      .. button-link:: continuous_integration/index.html
         :click-parent:
         :color: secondary
         :expand:

         Continuous integration

   .. grid-item-card:: Release
      :class-card: contrib-card
      :shadow: md

      Find detailed information on how to perform a release
      for Apache Arrow
      +++

      .. button-link:: release.html
         :click-parent:
         :color: secondary
         :expand:

         Release Management Guide

.. _toc.contributing:

.. toctree::
   :maxdepth: 2
   :caption: Contributing
   :hidden:

   contributing

.. _toc.developing:

.. toctree::
   :maxdepth: 1
   :caption: Development
   :hidden:

   cpp/index
   java/index
   python

.. toctree::
   :maxdepth: 1
   :caption: CI
   :hidden:

   continuous_integration/index
   benchmarks

.. toctree::
   :maxdepth: 2
   :caption: Documentation
   :hidden:

   documentation

.. toctree::
   :maxdepth: 2
   :caption: Release
   :hidden:

   release
