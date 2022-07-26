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

=========
Substrait
=========

In order to use Acero you will need to create an execution plan.  This is the
model that describes the computation you want to apply to your data.  Acero has
its own internal representation for execution plans but most users should not
interact with this directly as it will couple their code to Acero.

`Substrait <https://substrait.io>`_ is an open standard for execution plans.
Acero implements the Substrait "consumer" interface.  This means that Acero can
accept a Substrait plan and fulfill the plan, loading the requested data and
applying the desired computation.  By using Substrait plans users can easily
switch out to a different execution engine at a later time.

Substrait Conformance
---------------------

Substrait defines a broad set of operators and functions for many different
situations and it is unlikely that Acero will ever completely satisfy all
defined Substrait operators and functions.  To help understand what features
are available the following sections define which features have been currently
implemented in Acero and any caveats that apply.

Plans
^^^^^

 * A plan should have a single top-level relation.
 * The consumer is currently based on a custom build of Substrait that
   is older than 0.1.0.  Any features added that are newer than 0.1.0 will
   not be supported.

Extensions
^^^^^^^^^^

 * If a plan contains any extension type variations it will be rejected.
 * If a plan contains any advanced extensions it will be rejected.

Relations (in general)
^^^^^^^^^^^^^^^^^^^^^^

 * The ``emit`` property (to customize output order of a node or to drop
   columns) is not supported and plans containing this property will
   be rejected.
 * The ``hint`` property is not supported and plans containing this
   property will be rejected.
 * Any advanced extensions will cause a plan to be rejected.
 * Any relation not explicitly listed below will not be supported
   and will cause the plan to be rejected.

Read Relations
^^^^^^^^^^^^^^

 * The ``projection`` property is not supported and plans containing this
   property will be rejected.
 * The only supported read type is ``LocalFiles``.  Plans with any other
   type will be rejected.
 * Only the parquet file format is currently supported.
 * All URIs must use the ``file`` scheme
 * ``partition_index``, ``start``, and ``length`` are not supported.  Plans containing
   these properties will be rejected.
 * The Substrait spec requires that a ``filter`` be completely satisfied by a read
   relation.  However, Acero only uses a read filter for pushdown projection and
   it may not be fully satisfied.  Users should generally attach an additional
   filter relation with the same filter expression after the read relation.

Filter Relations
^^^^^^^^^^^^^^^^

 * No know caveats

Project Relations
^^^^^^^^^^^^^^^^^

 * No known caveats

Join Relations
^^^^^^^^^^^^^^

 * The join type ``JOIN_TYPE_SINGLE`` is not supported and plans containing this
   will be rejected.
 * The join expression must be a call to either the ``equal`` or ``is_not_distinct_from``
   functions.  Both arguments to the call must be direct references.  Only a single
   join key is supported.
 * The ``post_join_filter`` property is not supported and will be ignored.

Expressions (general)
^^^^^^^^^^^^^^^^^^^^^

 * Various places in the Substrait spec allow for expressions to be used outside
   of a filter or project relation.  For example, a join expression or an aggregate
   grouping set.  Acero typically expects these expressions to be direct references.
   Planners should extract the implicit projection into a formal project relation
   before delivering the plan to Acero.

Literals
^^^^^^^^

 * A literal with non-default nullability will cause a plan to be rejected.

Functions
^^^^^^^^^

 * The only functions currently supported by Acero are:

   * add
   * equal
   * is_not_distinct_from

 * The functions above must be referenced using the URI
   ``https://github.com/apache/arrow/blob/master/format/substrait/extension_types.yaml``