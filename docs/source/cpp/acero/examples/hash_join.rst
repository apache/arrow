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
.. cpp:namespace:: arrow::compute

.. _stream_execution_hashjoin_docs:

=========
Hash Join
=========

``hash_join`` operation provides the relational algebra operation, join using hash-based
algorithm. :class:`arrow::compute::HashJoinNodeOptions` contains the options required in 
defining a join. The hash_join supports 
`left/right/full semi/anti/outerjoins
<https://en.wikipedia.org/wiki/Join_(SQL)>`_. 
Also the join-key (i.e. the column(s) to join on), and suffixes (i.e a suffix term like "_x"
which can be appended as a suffix for column names duplicated in both left and right 
relations.) can be set via the the join options. 
`Read more on hash-joins
<https://en.wikipedia.org/wiki/Hash_join>`_. 

Example:

.. literalinclude:: ../../../../../cpp/examples/arrow/execution_plan_documentation_examples.cc
  :language: cpp
  :start-after: (Doc section: HashJoin Example)
  :end-before: (Doc section: HashJoin Example)
  :linenos:
  :lineno-match:
