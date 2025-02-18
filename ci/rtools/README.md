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

# rtools40 patches for AWS SDK and related libs

The patches in this directory are solely for the purpose of building Arrow C++
under [Rtools40](https://cran.r-project.org/bin/windows/Rtools/rtools40.html)
and not used elsewhere. Once we've dropped support for Rtools40, we can consider
removing these patches.

The larger reason these patches are needed is that Rtools provides their own
packages and their versions of the AWS libraries weren't compatible with CMake
3.25. Our solution was to bundle the AWS libs instead and these patches were
required to get them building under the Rtools40 environment.

The patches were added while upgrading the minimum required CMake version to
3.25 in [GH-44950](https://github.com/apache/arrow/issues/44950). Please see the
associated PR, [GH-44989](https://github.com/apache/arrow/pull/44989), for more
context.
