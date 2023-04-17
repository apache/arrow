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

How to Change Format Specification
==================================

**Cross-language** is important in Apache Arrow. To keep it, we use
the following process when dealing with format (files in
`<https://github.com/apache/arrow/tree/main/format>`_) changes:

* We must go through a formal DISCUSS and VOTE process
* We must have at least 2 reference implementations and associated
  integration tests

DISCUSS and VOTE Process
------------------------

DISCUSS process is a process to discuss the format changes in
public. Anyone can join the discussion. The DISCUSS process is done by
starting a thread in dev@arrow.apache.org with ``[DISCUSS]`` prefixed
subject.

.. note::

   We sometimes use ``[Discuss]``, ``DISCUSS: `` or something but
   ``[DISCUSS]`` is recommended.

Here are examples:

* `[Discuss][Format] Add 32-bit and 64-bit Decimals <https://lists.apache.org/thread/9ynjmjlxm44j2pt443mcr2hmdl7m43yz>`_
* `[DISCUSS][Format] Starting to do some concrete work on the new "StringView" columnar data type <https://lists.apache.org/thread/dccj1qrozo88qsxx133kcy308qwfwpfm>`_

VOTE process is a process to tell whether we have reached
consensus. We can start a vote for the format changes after we reach
consensus in the preceding DISCUSS process.

See also: `Apache Voting Process <https://www.apache.org/foundation/voting.html>`_

At Least 2 Reference Implementations
------------------------------------

We must have at least 2 reference implementations and associated
integration tests to confirm whether the format changes are
cross-compatible and consistent.

We must choose complete implementations for them. Here are candidate
implementations:

* The C++ implementation
* The Java implementation

We can discuss and vote to add more implementations to the list.
We may use doc:`../status` for the discussion.
