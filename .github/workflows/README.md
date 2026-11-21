<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Merge queue

`merge_queue.yml` supplies the required **Merge Queue** check for `main`.
On pull requests it permits entry into the queue without repeating the existing
PR workflows. On `merge_group`, it runs every regular language and tooling
workflow against the candidate containing current `main` and the queued change.
Every called workflow must succeed; failure, cancellation, and skipped suites
block merging. Extra, packaging, and release workflows retain their separate
triggers and are not part of this gate.

The `.asf.yaml` configuration activates the queue with one candidate build at a
time and a six-hour check timeout, including runner scheduling. It leaves the
existing PR and post-merge CI in place. This first step adds validation before
merge; it does not reduce CI usage. Queue branch pushes are excluded so they
cannot start duplicate builds.

Use GitHub's merge queue UI to enqueue a reviewed PR. `dev/merge_arrow_pr.py`
uses the direct merge API and cannot enqueue or wait for a queued merge. Close
the associated issue and set its milestone after the queued PR actually merges.
Maintenance branches retain their existing merge procedure.

To exercise the same build suite on a fork with the workflow installed:

```shell
gh workflow run merge_queue.yml --repo OWNER/arrow --ref BRANCH
```

GitHub requires an organization-owned repository for a native merge queue.
A manual fork run checks the suite and result aggregation, but does not test
queue ordering, stale-candidate replacement, or automatic merging. Validate
those behaviors and runner availability before enabling the proposed rules in
Apache Arrow. Queue settings follow the
[GitHub merge queue documentation](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/configuring-pull-request-merges/managing-a-merge-queue).
