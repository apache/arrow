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

# Conda Forge recipes

This directory must be migrated periodically with the upstrem updates of
[arrow-cpp-feedstock][arrow-cpp-feedsotkc],
[parquet-cpp-feedstock][parquet-cpp-feedstock] and
[pyarrow-feedstock][pyarrow-feedstock]
conda-forge repositories because of multiple vendored files.

## Keeping the recipes synchronized

The recipes here are tested on nightly basis, so they follow the development
versions of arrow instead of the upstream recipes, which are suitable for the
latest releases.

### Backporting from the upstream feedstocks

In most of the cases these recipes are more accurate, then the upstream
feedstocks. Altough the upstream feedstocks regurarly receive automatic updates
by the conda-forge team so we need to backport those changes to the crossbow
recipes. Most of these updates are touching the version pinning files
(under `.ci_support`) and other CI related configuration files.

Because all three recipes must be built in the same continuous integration
job prefer porting from the [pyarrow feedstock][pyarrow-feedstock].

#### Updating the variants:

Copy the configuration files from `pyarrow-feedstock/.ci_support` to the
`.ci_support` folder.

#### Updating the CI configurations:

The `.azure-pipelines/azure-pipelines-[linux|osx|win].yml` should be ported
to the local counterparts under `.azure-pipelines` with keeping the crossbow
related parts (the cloning of arrow and the jinja templated variables) and
moving the matrix definitions like [this][matrix-definition] to the crossbow
[tasks.yml][../tasks.yml] config file.


### Porting recipes from crossbow to the upstream feedstocks

Theoretically these recipes should be up to date with the actual version of
Arrow, so during the release procedure the content of these recipes should be
copied to the upstream feedstocks.


[arrow-cpp-feedstock]: https://github.com/conda-forge/arrow-cpp-feedstock
[parquet-cpp-feedstock]: https://github.com/conda-forge/parquet-cpp-feedstock
[pyarrow-cpp-feedstock]: https://github.com/conda-forge/pyarrow-feedstock
[matrix-definition]: https://github.com/conda-forge/pyarrow-feedstock/blob/master/.azure-pipelines/azure-pipelines-linux.yml#L12
