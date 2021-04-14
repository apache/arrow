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
# Integration Testing

Ballista has a [benchmark crate](https://github.com/ballista-compute/ballista/tree/main/rust/benchmarks/tpch) which is
derived from TPC-H and this is currently the main form of integration testing. 

The following command can be used to run the integration tests.

```bash
./dev/integration-tests.sh
```

Please refer to the
[benchmark documentation](https://github.com/ballista-compute/ballista/blob/main/rust/benchmarks/tpch/README.md)
for more information.
