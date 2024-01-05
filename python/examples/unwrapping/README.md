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

# Wrapping and Unwrapping Pyarrow Objects

This is some arbitrary code to demonstrate [wrapping and
unwrapping](https://arrow.apache.org/docs/python/integration/extending.html)
functionality. This tooling allows you to interop pyarrow objects with Cython or Arrow
C++.

Instructions:

```
python setup.py build_ext --inplace

python run_demo.py
```
