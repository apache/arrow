<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
## Benchmark Builds Env
This directory contains 
- [benchmarks.env](../../dev/conbench_envs/benchmarks.env) - list of env vars used for building Arrow C++/Python/R/Java/JavaScript and running benchmarks using [conbench](https://ursalabs.org/blog/announcing-conbench/)
- [utils.sh](../../dev/conbench_envs/utils.sh) - hooks used by benchmark builds (triggered by `@ursabot please benchmark` PR comments) managed in Ursa's private repo. Benchmark builds use these hooks to create conda env with Arrow C++/Python/R/Java/JavaScript built from source.

Defining hooks in [utils.sh](../../dev/conbench_envs/utils.sh) in Arrow repo allows benchmark builds for a specific Arrow commit to be always compatible with Arrow's files/scripts used for installing Arrow dependencies and building Arrow, assuming Arrow contributors will update utils.sh when they make these changes to files/scripts used by functions in utils.sh. 
 

## How to add or update Arrow build and run env vars used by benchmark builds
1. Create `apache/arrow` PR
2. Update or add env var value in [benchmarks.env](../../dev/conbench_envs/benchmarks.env)
3. Add `@ursabot please benchmark` comment to PR
4. Once benchmark builds are done, benchmark results can be viewed via compare/runs links in the PR comment where
- baseline = PR base HEAD commit with unaltered `/dev/conbench_envs/benchmarks.env`
- contender = PR branch HEAD commit with overridden `/dev/conbench_envs/benchmarks.env`