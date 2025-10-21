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

Changing the Apache Arrow Format Specification
==============================================

**Cross-language** compatibility is important in Apache Arrow. To
maintain it, we use the following process when dealing with changes to
the format (files in
`<https://github.com/apache/arrow/tree/main/format>`_):

* We must discuss and vote on the changes on the public mailing list
* We must have at least two reference implementations and associated
  integration tests

These do not have to be done in order. In most cases, having at least
one draft reference implementation is helpful for design discussion.

.. note::

   We must update the corresponding documentation (files in
   `<https://github.com/apache/arrow/tree/main/docs/source/format>`_)
   too.

Discussion and Voting Process
-----------------------------

Changes to the format should be discussed on the public mailing list.
Anyone can join the discussion. The discussion should be started by a
thread in dev@arrow.apache.org with the ``[DISCUSS]`` prefixed
subject.

.. note::

   We sometimes use ``[Discuss]``, ``DISCUSS:`` or something similar but
   ``[DISCUSS]`` is recommended.

Here are some examples:

* `[Discuss][Format] Add 32-bit and 64-bit Decimals <https://lists.apache.org/thread/9ynjmjlxm44j2pt443mcr2hmdl7m43yz>`_
* `[DISCUSS][Format] Starting to do some concrete work on the new "StringView" columnar data type <https://lists.apache.org/thread/dccj1qrozo88qsxx133kcy308qwfwpfm>`_

The voting process is used to verify we have reached consensus. We can
start a vote for the format changes after we reach consensus in the
preceding DISCUSS mailing list thread. Similar to discussion threads,
voting thread must have the subject prefix ``[VOTE]``.

See also: `Apache Voting Process <https://www.apache.org/foundation/voting.html>`_

At Least Two Reference Implementations
--------------------------------------

We must have at least two reference implementations and associated
integration tests to confirm whether the format changes are compatible
across languages and consistent.

Reference implementations must be within complete Arrow
implementations. For example, the C++ library is acceptable but the
Python library is not, since it is a wrapper around the C++
library. Here are candidate implementations:

* The C++ implementation
* The Java implementation
* The Rust (arrow-rs) implementation
* The Go implementation

We can discuss and vote to add more implementations to the list. We
may use :doc:`../status` to determine which implementations are
complete.

Versioning
----------

The format version (which is separate from the library versions) must
also be incremented as new changed are made. See :doc:`Versioning`.
