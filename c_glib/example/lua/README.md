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

# Arrow Lua example

There are Lua example codes in this directory.

## How to run

All example codes use [LGI](https://github.com/pavouk/lgi) to use
Arrow GLib based bindings.

Here are command lines to install LGI on Debian GNU/Linux and Ubuntu:

```console
$ sudo apt install -y luarocks
$ sudo luarocks install lgi
```

## Lua example codes

Here are example codes in this directory:

  * `write-file.lua`: It shows how to write Arrow array to file in
    file format.

  * `read-file.lua`: It shows how to read Arrow array from file in
    file format.

  * `write-stream.lua`: It shows how to write Arrow array to file in
    stream format.

  * `read-stream.lua`: It shows how to read Arrow array from file in
    stream format.
