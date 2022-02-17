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

**********************************************
Arrow Compute IR (Intermediate Representation)
**********************************************

In the same way that the Arrow format provides a powerful tool
for communicating data, Compute IR is intended to provide a
consistent format for representing analytical operations against
that data. As an arrow-native expression of computation it includes
information such as explicit types and schemas and arrow formatted
literal data. It is also optimized for low runtime overhead in both
serialization and deserialization.

Built-in definitions are included to enable representation of
relational algebraic operations- the contents of a "logical query plan".
Compute IR also has first class support for representing operations
which are not members of a minimal relational algebra, including
implementation and optimization details- the contents of a "physical
query plan". This approach is taken in emulation of `MLIR`_ (Multi-Level
Intermediate Representation), a system which has had strong successes in
spaces of comparable complexity to representation of analytic operations.
To borrow terms from that project, there are two mutations of interest:

* Replacement of representations with semantically equivalent representations
  which will yield better performance for consumers- an optimization pass.
* Replacement of abstract or generic representations with more specific
  and potentially consumer-specific representations- a lowering pass.
  This modification corresponds to the translation of a logical plan
  to a physical plan.

Allowing representation of physical plans (and plans which are between
logical and physical) in Compute IR enables systems to define incremental
optimization and lowering passes which operate on and produce valid
Compute IR. This in turn enables communication, manipulation, and inspection
at every stage of lowering/optimization by the same tools
used for logical-plan-equivalent-IR. This is especially useful for systems
where such passes may depend on information only available on every node
of a distributed consumer (for example statistics unique to that node's
local data) or may not be universal to all backends in a heterogeneous
consumer (for example which optimizations nodes are capable of for
non equi joins).

.. _MLIR: https://mlir.llvm.org
