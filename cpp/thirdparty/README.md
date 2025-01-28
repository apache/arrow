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

# Arrow C++ Thirdparty Dependencies

See the "Build Dependency Management" section in the [C++ Developer
Documentation][1].

[1]: https://github.com/apache/arrow/blob/main/docs/source/developers/cpp/building.rst

## Update versions automatically

There is a convenient script that update versions in `versions.txt` to
the latest version automatically. You can use it like the following:

```console
cpp/thirdparty/update.rb PRODUCT_PATTERN1 PRODUCT_PATTERN2 ...
```

For example, you can update AWS SDK for C++ related products' versions
by the following command line:

```console
cpp/thirdparty/update.rb "AWS*" "S2N*"
```
