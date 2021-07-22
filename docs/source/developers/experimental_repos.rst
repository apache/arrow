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

Experimental repositories
=========================

Apache Arrow has an explicit policy over developing experimental repositories
in the context of
`rules for revolutionaries <https://grep.codeconsult.ch/2020/04/07/rules-for-revolutionaries-2000-edition/>`_.

The main motivation for this policy is to offer a lightweight mechanism to
conduct experimental work, with the necessary creative freedom, within the ASF
and the Apache Arrow governance model. This policy allows committers to work on
new repositories, as they offer many important tools to manage it (e.g. github
issues, “watch”, “github stars” to measure overall interest).

Process
+++++++

* A committer *may* initiate experimental work by creating a separate git
  repository within the Apache Arrow (e.g. via `selfserve <https://selfserve.apache.org/>`_)
  and announcing it on the mailing list, together with its goals, and a link to the
  newly created repository.
* The committer *must* initiate an email thread with the sole purpose of
  presenting updates to the community about the status of the repo.
* There *must not* be official releases from the repository.
* Any decision to make the experimental repo official in any way, whether by merging or migrating, *must* be discussed and voted on in the mailing list.
* The committer is responsible for managing issues, documentation, CI of the repository,
  including licensing checks.
* The committer decides when the repository is archived.

Repository management
+++++++++++++++++++++

* The repository *must* be under ``apache/``
* The repository’s name *must* be prefixed by ``arrow-experimental-``
* The committer has full permissions over the repository (within possible in ASF)
* Push / merge permissions *must only* be granted to Apache Arrow committers

Development process
+++++++++++++++++++

* The repository must follow the ASF requirements about 3rd party code.
* The committer decides how to manage issues, PRs, etc.

Divergences
+++++++++++

* If any of the “must” above fails to materialize and no correction measure
  is taken by the committer upon request, the PMC *should* take ownership
  and decide what to do.
