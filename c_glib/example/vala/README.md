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

# Arrow Vala example

There are Vala example codes in this directory.

## How to build

Here is a command line to build an example in this directory:

```console
$ valac --pkg arrow-glib --pkg posix XXX.vala
```

## Vala example codes

Here are example codes in this directory:

  * `build.vala`: It shows how to create an array by array builder.

  * `write-file.vala`: It shows how to write Arrow array to file in
    file format.

  * `read-file.vala`: It shows how to read Arrow array from file in
    file format.

  * `write-stream.vala`: It shows how to write Arrow array to file in
    stream format.

  * `read-stream.vala`: It shows how to read Arrow array from file in
    stream format.
