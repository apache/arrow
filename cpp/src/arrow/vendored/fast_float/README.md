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

# fast_float

The files in this directory are vendored from fast_float
git tag `v8.1.0`.

See https://github.com/fastfloat/fast_float

## Changes

- enclosed in `arrow_vendored` namespace.

## How to update

You must replace `VERSION` in the command line below with suitable version
such as `v8.1.0`.

```bash
cpp/src/arrow/vendored/fast_float/update.sh VERSION
git commit add cpp/src/arrow/vendoered/fast_float/
```
