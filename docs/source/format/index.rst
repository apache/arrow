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

Format documentation
====================

Welcome to the specifications and protocols documentation!

.. grid:: 2
   :gutter: 4
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: C data interface
      :class-card: contrib-card
      :shadow: md

      Very small, stable set of C definitions that can be
      easily copied in any project’s source code and used
      for columnar data interchange in the Arrow format.

      +++

      .. button-link:: CDataInterface.html
         :click-parent:
         :color: secondary
         :expand:

         The Arrow C data interface

   .. grid-item-card:: C stream interface
      :class-card: contrib-card
      :shadow: md

      Easing the communication of streaming data structures,
      defined in the C data interface, within a single process.

      +++

      .. button-link:: CStreamInterface.html
         :click-parent:
         :color: secondary
         :expand:

         The Arrow C stream interface

.. dropdown:: Format specifications
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md
   :open:

   * :doc:`Versioning`
   * :doc:`Columnar`
   * :doc:`CanonicalExtensions`
   * :doc:`Other`
   * :doc:`Changing`

.. dropdown:: Apache Arrow Protocols
   :animate: fade-in-slide-down
   :class-title: sd-fs-5
   :class-container: sd-shadow-md
   :open:

   * :doc:`Flight`
   * :doc:`FlightSql`
   * :doc:`ADBC`

.. grid:: 2
   :padding: 2 2 0 0
   :class-container: sd-text-center

   .. grid-item-card:: Terminology
      :class-card: contrib-card
      :shadow: md

      List of common terms with a short description
      applicable to the Apache Arrow project.

      +++

      .. button-link:: Glossary.html
         :click-parent:
         :color: secondary
         :expand:

         Glossary

   .. grid-item-card:: Integration
      :class-card: contrib-card
      :shadow: md

      Strategy for integration testing between
      Arrow implementations

      +++

      .. button-link:: Integration.html
         :click-parent:
         :color: secondary
         :expand:

         Integration Testing

.. toctree::
   :maxdepth: 2
   :caption: Specifications and Protocols
   :hidden:

   Versioning
   Columnar
   CanonicalExtensions
   Other
   CDataInterface
   CStreamInterface
   Flight
   FlightSql
   ADBC
   Changing
   Integration
   Glossary
