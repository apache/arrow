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

# Generating Arrow Flatbuffers code in Rust

I compiled flatbuffers locally from commit 21591916afea4f50bb448fd071c3fccbc1d8034f and ran these commands to generate the source files:

```bash
flatc --rust ../format/File.fbs
flatc --rust ../format/Schema.fbs
flatc --rust ../format/Message.fbs
flatc --rust ../format/Tensor.fbs
```

There seems to be a bug in the current Flatbuffers code in the Rust implementation, so I had to manually search and replace to change `type__type` to `type_type`.

I also removed the generated namespace `org::apache::arrow::format` and had to manually add imports at the top of each file.