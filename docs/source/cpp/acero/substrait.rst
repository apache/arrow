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

.. default-domain:: cpp
.. highlight:: cpp
.. cpp:namespace:: arrow::engine::substrait

.. _acero-substrait:

==========================
Using Acero with Substrait
==========================

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
* The consumer is currently based on version 0.20.0 of Substrait.
  Any features added that are newer will not be supported.
* Due to a breaking change in 0.20.0 any Substrait plan older than 0.20.0
  will be rejected.

Extensions
^^^^^^^^^^

* If a plan contains any extension type variations it will be rejected.
* Advanced extensions can be provided by supplying a custom implementation of
  :class:`arrow::engine::ExtensionProvider`.

Relations (in general)
^^^^^^^^^^^^^^^^^^^^^^

* Any relation not explicitly listed below will not be supported
  and will cause the plan to be rejected.

Read Relations
^^^^^^^^^^^^^^

* The ``projection`` property is not supported and plans containing this
  property will be rejected.
* The ``VirtualTable`` and ``ExtensionTable`` read types are not supported.
  Plans containing these types will be rejected.
* Only the parquet and arrow file formats are currently supported.
* All URIs must use the ``file`` scheme
* ``partition_index``, ``start``, and ``length`` are not supported.  Plans containing
  non-default values for these properties will be rejected.
* The Substrait spec requires that a ``filter`` be completely satisfied by a read
  relation.  However, Acero only uses a read filter for pushdown projection and
  it may not be fully satisfied.  Users should generally attach an additional
  filter relation with the same filter expression after the read relation.

Filter Relations
^^^^^^^^^^^^^^^^

* No known caveats

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

Aggregate Relations
^^^^^^^^^^^^^^^^^^^

* At most one grouping set is supported.
* Each grouping expression must be a direct reference.
* Each measure's arguments must be direct references.
* A measure may not have a filter
* A measure may not have sorts
* A measure's invocation must be AGGREGATION_INVOCATION_ALL or 
  AGGREGATION_INVOCATION_UNSPECIFIED
* A measure's phase must be AGGREGATION_PHASE_INITIAL_TO_RESULT

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

Types
^^^^^

* Acero does not have full support for non-nullable types and may allow input
  to have nulls without rejecting it.
* The table below shows the mapping between Arrow types and Substrait type
  classes that are currently supported

.. list-table:: Substrait / Arrow Type Mapping
   :widths: 25 25 50
   :header-rows: 1

   * - Substrait Type
     - Arrow Type
     - Caveat
   * - boolean
     - boolean
     - 
   * - i8
     - int8
     - 
   * - i16
     - int16
     - 
   * - i32
     - int32
     - 
   * - i64
     - int64
     - 
   * - fp32
     - float32
     - 
   * - fp64
     - float64
     - 
   * - string
     - string
     - 
   * - binary
     - binary
     - 
   * - timestamp
     - timestamp<MICRO,"">
     - 
   * - timestamp_tz
     - timestamp<MICRO,"UTC">
     - 
   * - date
     - date32<DAY>
     - 
   * - time
     - time64<MICRO>
     - 
   * - interval_year
     - 
     - Not currently supported
   * - interval_day
     - 
     - Not currently supported
   * - uuid
     - 
     - Not currently supported
   * - FIXEDCHAR<L>
     - 
     - Not currently supported
   * - VARCHAR<L>
     - 
     - Not currently supported
   * - FIXEDBINARY<L>
     - fixed_size_binary<L>
     - 
   * - DECIMAL<P,S>
     - decimal128<P,S>
     - 
   * - STRUCT<T1...TN>
     - struct<T1...TN>
     - Arrow struct fields will have no name (empty string)
   * - NSTRUCT<N:T1...N:Tn>
     - 
     - Not currently supported
   * - LIST<T>
     - list<T>
     - 
   * - MAP<K,V>
     - map<K,V>
     - K must not be nullable

Functions
^^^^^^^^^

* The following functions have caveats or are not supported at all.  Note that
  this is not a comprehensive list.  Functions are being added to Substrait at
  a rapid pace and new functions may be missing.

  * Acero does not support the SATURATE option for overflow
  * Acero does not support kernels that take more than two arguments
    for the functions ``and``, ``or``, ``xor``

* Substrait has not yet clearly identified the form that URIs should take for
  standard functions.  Acero will look for the URIs to the ``main`` Github branch.
  In other words, for the file ``functions_arithmetic.yaml`` Acero expects
  ``https://github.com/substrait-io/substrait/blob/main/extensions/functions_arithmetic.yaml``

  * Acero has functions that are not yet a part of Substrait (or may never be added
    as official functions).  To invoke these functions you can use the special URI
    ``urn:arrow:substrait_simple_extension_function``.  If this URI is encountered
    then Acero will match only on function name and will ignore any function options.

  * Alternatively, the URI can be left completely empty and Acero will match
    based only on function name.  This fallback mechanism is non-standard and should
    be considered deprecated in favor of the special URI above.
