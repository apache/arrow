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

# pyarrow_gandiva



## Development

To build the Cython components, use the `--target=pyarrow_gandiva` with
setup.py:

```shell
python setup.py build_ext --inplace --target=pyarrow_gandiva
```

To run tests, use pytest with `python -m`. If you run it directly, the main
`pyarrow` won't be on the import path and you will get import errors.

```shell
python -m pytest pyarrow_gandiva
```
