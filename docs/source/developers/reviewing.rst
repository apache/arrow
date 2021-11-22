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
being meticulous when reviewing brings greater rewards to the project than
being lenient and aiming for quick merges.

Fixing potential issues after a Pull Request (PR) merge is more costly since
it forces the developer to context-switch back to work that was thought
to be finished.  Moreover, an late-minute API change may propagate to
other parts of the project and require more attention than if done up front.
And there are social issues with asking a volunteer to go back to work
on something that was accepted as finished.

Guidelines
==========

Meta
----

**Use your common sense**.  These guidelines are not hard rules.
Committers are expected to have sufficient expertise on their work
areas to be able to modulate concerns if necessary.

These guidelines are not listed in a particular order.  They are
not intended to be used as a checklist.

Finally, **these guidelines are not currently exhaustive**.

Public API design
-----------------

- Public APIs should nudge users towards the most desirable constructs.
  In other words, if there is a "best" way to do something, it should
  ideally also be the most easily discoverable and the most concise to type.
  For example, safe APIs should ideally be featured more prominently than
  unsafe APIs that may crash or silently produce erronenous results on
  invalid input.

- Public APIs should ideally tend to produce readable code.  One example
  is when multiple options are expected to be added over time: it is better
  to try to organize options logically rather than juxtapose them all in
  a function's signature (see for example the CSV reading APIs in C++ and Python).

- Naming is important.  Try to ask yourself if code calling the new API
  would be understandable without having to read the API docs.
  Vague naming should be avoided; inaccurate naming is even worse as it
  can mislead the reader and lead to buggy user code.

- Be mindful of terminology.  Every project has (explicitly or tacitly) set
  conventions about how to name important concepts; steering away from those
  conventions increases the cognitive workload both for contributors and
  users of the project.  Conversely, reusing a well-known term for a different
  purpose than usual can also increase the cognitive workload and make
  developers' life generally more difficult.

- If it is unsure whether an API is the right one for the task at hand,
  it is advisable to mark it experimental, such that users know that it
  may be changed over time, while contributors are less wary of bringing
  code-breaking improvements.  However, experimental APIs should not be
  used as an excuse for eschewing basic API design principles.

Robustness
----------

- Arrow is a set of open source libraries and will be used in a very wide
  array of contexts (including fiddling with deliberately artificial data
  at a Jupyter interpreter prompt).  If you are writing a public API, make
  sure that it won't crash or produce undefined behaviour on unusual (but
  valid) inputs.

- When a non-trivial algorithm is implemented, defense-in-depth checks can
  be useful to catch potential problems (such as debug-only assertions, if
  the language allows them).

- APIs ingesting potentially untrusted data, such as on-disk file formats,
  should try to avoid crashing or produce silent bugs when invalid or
  corrupt data is fed to them.  This can require a lot of care that is
  out of the scope of regular code reviews (such as setting up fuzz testing
  - XXX link to current fuzz setup), but basic checks can still be suggested
  at the code review stage.

- When calling foreign APIs, especially system functions or APIs dealing with
  input / output, do check for errors and propagate them (if the language
  does not propagate errors automatically, such as C++).

Performance
-----------

- Think about performance, but do not obsess over it.  Algorithmic complexity
  is important if input size may be "large" (the meaning of large depends
  on the context: use your own expertise to decide!).  Micro-optimizations
  improving performance by 20% or more on performance-sensitive functionality
  may be useful as well; lesser micro-optimizations may not be worth the
  time spent on them, especially if they lead to more complicated code.

- If performance is important, measure it.  Do not satisfy yourself with
  guesses and intuitions (which may be founded on incorrect assumptions
  about the compiler or the hardware).

- Try to avoid trying to trick the compiler/interpreter/runtime by writing
  the code in a certain way, unless it's really important.  These tricks
  are generally dependent on platform details that may become obsolete,
  they are often brittle, and they can make code harder to maintain
  (a common question that can block contributors is "what should I do
  about this weird hack that my changes need to remove"?).

- Avoiding worst-case degenerescence (such as memory blowups when a size
  estimate is too imprecise) may be more critical than trying to improve
  the common case by a small amount.

Documentation
-------------

These guidelines should ideally apply to both prose documentation and
in-code docstrings.

- Look for ambiguous / sub-informative wording.  For example, "it is an error
  if ..." is less informative than either "An error is raised if ... " or
  "Behaviour is undefined if ..." (the first phrasing doesn't tell the
  reader what actually *happens* on such an error).

- Be mindful about spelling, grammar, expression, concision.  This may seem
  like an obvious concern, but not all people are not accustomed to care about
  this.

- Cross-linking increases the global value of documentation.  Sphinx especially
  has great cross-linking capabilities (including topic references, glossary
  terms, API references), be sure to make use of them!

Testing
-------

- When adding an API, all nominal cases should have test cases.  Does a function
  allow null values? Then null values should be tested (alongside non-null
  values, of course). Does a function allow different input types? etc.

- If some aspect of a functionality is delicate (either by definition or
  as an implementation detail), it should be tested.

- Corner cases should be exercised, especially in low-level implementation
  languages such as C++.  Examples: empty arrays, zero-chunk arrays, arrays
  with only nulls, etc.

- Stress tests can be useful, for example to uncover synchronizations bugs
  if non-trivial parallelization is being added, or to validate a computational
  argument against a slow and straightforward reference implementation.

- A mitigating concern, however, is the overall cost of running the test
  suite.  Continuous Integration (CI) runtimes can be painfully long and
  we should be wary of increasing them too much.  Sometimes it is
  worthwhile to fine-tune testing parameters to balance the usefulness
  of tests against the cost of running them (especially where stress tests
  are involved, since they tend to imply execution over large datasets).


Social aspects
==============

* Reviewing is a communication between the contributor and the reviewer.
  Avoid letting questions or comments unanswered for too long ("too long"
  is of course very subjective, but two weeks can be a reasonable
  heuristic).  If you cannot allocate time soon, do say it explicitly.
  If you don't have the answer to a question, do say it explicitly.
  Saying "I don't have time immediately but I will come back later,
  feel free to ping if I seem to have forgotten" or "Sorry, I am out of depth
  here" is always better than saying nothing and letting the interlocutor
  wondering.

* If you know someone who may help on a blocking issue and past experience
  suggests they can be available for that, feel free to add them to the
  discussion (for example by cc'ing their Github handle).

* If the contributor has stopped giving feedback or updating their PR,
  perhaps they're not interested anymore, but perhaps also they're stuck
  on some issue and feel unable to push their contribution any further.
  Don't hesitate to ask ("I see this PR hasn't seen any updates recently,
  are you stuck on something? Do you need any help?").

* If the contribution is genuinely desirable and the contributor is not making
  any progress, it is also possible to take it up.  Out of politeness,
  it is however better to ask the contributor first.

* Some contributors are looking for a quick fix to a specific problem and
  don't want to spend too much time on it.  Others on the contrary are eager
  to learn and improve their contribution to make it conform to the
  project's standards.  The latter kind of contributors are especially
  valuable as they may become long-term contributors or even committers
  to the project; it can therefore be a good strategy to prioritize
  interactions with such contributors.

* Some contributors may respond "I will fix it later, can we merge anyway?"
  when a problem is pointed.  Unfortunately, whether the fix is really
  contributed soon in later PR is difficult to predict or enforce.  If
  the contributor has shown to be trustable, it may be acceptable to do
  as suggested.  Otherwise, it is better to decline the suggestion.

* If a PR is generally ready for merge apart from trivial or uncontroversial
  concerns, the reviewer may decide to push changes themselves to the
  PR instead of asking the contributor to make the changes.


XXX There may be other things to add here, any suggestions?

