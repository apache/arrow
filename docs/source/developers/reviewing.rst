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

=======================
Reviewing contributions
=======================

Principles
==========

Arrow is a foundational project that will need to evolve over many years
or even decades, while serving potentially millions of users.  We believe
that being meticulous when reviewing brings greater rewards to the project
than being lenient and aiming for quick merges.

Code reviews like this lead to better quality code, more people who are
engaged with and understand the code being changed, and a generally
healthier project with more room to grow and accommodate emerging needs.


Guidelines
==========

Meta
----

**Use your own judgement**.  These guidelines are not hard rules.
Committers are expected to have sufficient expertise on their work
areas to be able to adjust their approach based on any concerns they have.

These guidelines are not listed in a particular order and are not intended
to be used as a checklist.

Finally, these guidelines are not exhaustive.

Scope and completeness
----------------------

* Our general policy is to not introduce regressions or merge PRs that require
  follow-ons to function correctly (though exceptions to this can be made).
  Making necessary changes after a merge is more costly both for the
  contributor and the reviewer, but also for other developers who may be
  confused if they hit problems introduced by a merged PR.

* What changes are in-scope for a PR and what changes might/could/should be
  pushed out of scope and have a follow-up issue created should be determined
  in collaboration between the authors and the reviewers.

* When a large piece of functionality is being contributed and it seems
  desirable to integrate it piecewise, favour functional cohesion when
  deciding how to divide changes (for example, if a filesystem implementation
  is being contributed, a first PR may contribute directory metadata
  operations, a second PR file reading facilities and a third PR file writing
  facilities).

Public API design
-----------------

* Public APIs should nudge users towards the most desirable constructs.
  In other words, if there is a "best" way to do something, it should
  ideally also be the most easily discoverable and the most concise to type.
  For example, safe APIs should be featured more prominently than
  unsafe APIs that may crash or silently produce erroneous results on
  invalid input.

* Public APIs should ideally tend to produce readable code.  One example
  is when multiple options are expected to be added over time: it is better
  to try to organize options logically rather than juxtapose them all in
  a function's signature (see for example the CSV reading APIs in
  :ref:`C++ <cpp-api-csv>` and :ref:`Python <py-api-csv>`).

* Naming is important.  Try to ask yourself if code calling the new API
  would be understandable without having to read the API docs.
  Vague naming should be avoided; inaccurate naming is even worse as it
  can mislead the reader and lead to buggy user code.

* Be mindful of terminology.  Every project has (explicitly or tacitly) set
  conventions about how to name important concepts; steering away from those
  conventions increases the cognitive workload both for contributors and
  users of the project.  Conversely, reusing a well-known term for a different
  purpose than usual can also increase the cognitive workload and make
  developers' lives more difficult.

* If you are unsure whether an API is the right one for the task at hand,
  it is advisable to mark it experimental, such that users know that it
  may be changed over time, while contributors are less wary of bringing
  code-breaking improvements.  However, experimental APIs should not be
  used as an excuse for eschewing basic API design principles.

Robustness
----------

* Arrow is a set of open source libraries that will be used in a very wide
  array of contexts (including fiddling with deliberately artificial data
  at a Jupyter interpreter prompt).  If you are writing a public API, make
  sure that it won't crash or produce undefined behaviour on unusual (but
  valid) inputs.

* When a non-trivial algorithm is implemented, defensive coding can
  be useful to catch potential problems (such as debug-only assertions, if
  the language allows them).

* APIs ingesting potentially untrusted data, such as on-disk file formats,
  should try to avoid crashing or produce silent bugs when invalid or
  corrupt data is fed to them.  This can require a lot of care that is
  out of the scope of regular code reviews (such as setting up
  :ref:`fuzz testing <cpp-fuzzing>`), but basic checks can still be
  suggested at the code review stage.

* When calling foreign APIs, especially system functions or APIs dealing with
  input / output, do check for errors and propagate them (if the language
  does not propagate errors automatically, such as C++).

Performance
-----------

* Think about performance, but do not obsess over it.  Algorithmic complexity
  is important if input size may be "large" (the meaning of large depends
  on the context: use your own expertise to decide!).  Micro-optimizations
  improving performance by 20% or more on performance-sensitive functionality
  may be useful as well; lesser micro-optimizations may not be worth the
  time spent on them, especially if they lead to more complicated code.

* If performance is important, measure it.  Do not satisfy yourself with
  guesses and intuitions (which may be founded on incorrect assumptions
  about the compiler or the hardware).

  .. seealso:: :ref:`Benchmarking Arrow <benchmarks>`

* Try to avoid trying to trick the compiler/interpreter/runtime by writing
  the code in a certain way, unless it's really important.  These tricks
  are generally brittle and dependent on platform details that may become
  obsolete, and they can make code harder to maintain (a common question
  that can block contributors is "what should I do about this weird hack
  that my changes would like to remove"?).

* Avoiding rough edges or degenerate behaviour (such as memory blowups when
  a size estimate is inaccurately large) may be more important than trying to
  improve the common case by a small amount.

Documentation
-------------

These guidelines should ideally apply to both prose documentation and
in-code docstrings.

* Look for ambiguous / poorly informative wording.  For example, *"it is an
  error if ..."* is less informative than either *"An error is raised if ... "*
  or *"Behaviour is undefined if ..."* (the first phrasing doesn't tell the
  reader what actually *happens* on such an error).

* When reviewing documentation changes (or prose snippets, in general),
  be mindful about spelling, grammar, expression, and concision.  Clear
  communication is essential for effective collaboration with people
  from a wide range of backgrounds, and contributes to better documentation.

* Some contributors do not have English as a native language (and perhaps
  neither do you).  It is advised to help them and/or ask for external help
  if needed.

* Cross-linking increases the global value of documentation.  Sphinx especially
  has great cross-linking capabilities (including topic references, glossary
  terms, API references), so be sure to make use of them!

Testing
-------

* When adding an API, all nominal cases should have test cases.  Does a function
  allow null values? Then null values should be tested (alongside non-null
  values, of course). Does a function allow different input types? etc.

* If some aspect of a functionality is delicate (either by definition or
  as an implementation detail), it should be tested.

* Corner cases should be exercised, especially in low-level implementation
  languages such as C++.  Examples: empty arrays, zero-chunk arrays, arrays
  with only nulls, etc.

* Stress tests can be useful, for example to uncover synchronizations bugs
  if non-trivial parallelization is being added, or to validate a computational
  argument against a slow and straightforward reference implementation.

* A mitigating concern, however, is the overall cost of running the test
  suite.  Continuous Integration (CI) runtimes can be painfully long and
  we should be wary of increasing them too much.  Sometimes it is
  worthwhile to fine-tune testing parameters to balance the usefulness
  of tests against the cost of running them (especially where stress tests
  are involved, since they tend to imply execution over large datasets).


Social aspects
==============

* Reviewing is a communication between the contributor and the reviewer.
  Avoid letting questions or comments remain unanswered for too long
  ("too long" is of course very subjective, but two weeks can be a reasonable
  heuristic).  If you cannot allocate time soon, do say it explicitly.
  If you don't have the answer to a question, do say it explicitly.
  Saying *"I don't have time immediately but I will come back later,
  feel free to ping if I seem to have forgotten"* or *"Sorry, I am out of
  my depth here"* is always better than saying nothing and leaving the
  other person wondering.

* If you know someone who has the competence to help on a blocking issue
  and past experience suggests they may be willing to do so, feel free to
  add them to the discussion (for example by gently pinging their Github
  handle).

* If the contributor has stopped giving feedback or updating their PR,
  perhaps they're not interested any more, but perhaps also they're stuck
  on some issue and feel unable to push their contribution any further.
  Don't hesitate to ask (*"I see this PR hasn't seen any updates recently,
  are you stuck on something? Do you need any help?"*).

* If the contribution is genuinely desirable and the contributor is not making
  any progress, it is also possible to take it up.  Out of politeness,
  it is however better to ask the contributor first.

* Some contributors are looking for a quick fix to a specific problem and
  don't want to spend too much time on it.  Others on the contrary are eager
  to learn and improve their contribution to make it conform to the
  project's standards.  The latter kind of contributors are especially
  valuable as they may become long-term contributors or even committers
  to the project.

* Some contributors may respond *"I will fix it later, can we merge anyway?"*
  when a problem is pointed out to them.  Unfortunately, whether the fix will
  really be contributed soon in a later PR is difficult to predict or enforce.
  If the contributor has previously demonstrated that they are reliable,
  it may be acceptable to do as suggested.  Otherwise, it is better to
  decline the suggestion.

* If a PR is generally ready for merge apart from trivial or uncontroversial
  concerns, the reviewer may decide to push changes themselves to the
  PR instead of asking the contributor to make the changes.

* Ideally, contributing code should be a rewarding process.  Of course,
  it will not always be, but we should strive to reduce contributor
  frustration while keeping the above issues in mind.

* Like any communication, code reviews are governed by the Apache
  `Code of Conduct <https://www.apache.org/foundation/policies/conduct.html>`_.
  This applies to both reviewers and contributors.


Labelling
=========

While reviewing PRs, we should try to identify whether the corresponding issue 
needs to be marked with one or both of the following issue labels:

* **Critical Fix**: The change fixes either: (a) a security vulnerability;
  (b) a bug that causes incorrect or invalid data to be produced;
  or (c) a bug that causes a crash (while the API contract is upheld).
  This is intended to mark fixes to issues that may affect users without their
  knowledge. For this reason, fixing bugs that cause errors don't count, since 
  those bugs are usually obvious. Bugs that cause crashes are considered critical
  because they are a possible vector of Denial-of-Service attacks.
* **Breaking Change**: The change breaks backwards compatibility in a public API.
  For changes in C++, this does not include changes that simply break ABI
  compatibility, except for the few places where we do guarantee ABI
  compatibility (such as C Data Interface). Experimental APIs are *not*
  exempt from this; they are just more likely to be associated with this tag.
  
Breaking changes and critical fixes are separate: breaking changes alter the
API contract, while critical fixes make the implementation align with the
existing API contract. For example, fixing a bug that caused a Parquet reader
to skip rows containing the number 42 is a critical fix but not a breaking change,
since the row skipping wasn't behavior a reasonable user would rely on.

These labels are used in the release to highlight changes that users ought to be
aware of when they consider upgrading library versions. Breaking changes help
identify reasons when users may wish to wait to upgrade until they have time to
adapt their code, while critical fixes highlight the risk in *not* upgrading.

In addition, we use the following labels to indicate priority:

* **Priority: Blocker**: Indicates the changes **must** be merged before the next
  release can happen. This includes fixes to test or packaging failures that
  would prevent the release from succeeding final packaging or verification.
* **Priority: Critical**: Indicates issues that are high priority. This is a
  superset of issues marked "Critical Fix", as it also contains certain fixes
  to issues causing errors and crashes.
