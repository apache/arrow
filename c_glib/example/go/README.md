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

# Arrow Go example

There are Go example codes in this directory.

## How to run

All example codes use
[go-gir-generator](https://github.com/linuxdeepin/go-gir-generator) to
use Arrow GLib based bindings.

See [../../README.md](../../README.md) how to install Arrow GLib. You
can use packages to install Arrow GLib. The following instructions
assumes that you've installed Arrow GLib by package. Package name is
`libarrow-glib-dev` on Debian GNU/Linux and Ubuntu, `arrow-glib-devel`
on CentOS.

Here are command lines to install go-gir-generator on Debian GNU/Linux
and Ubuntu:

```text
% sudo apt install -V -y libarrow-glib-dev golang git libgirepository1.0-dev libgudev-1.0-dev
% export GOPATH=$HOME
% go get github.com/linuxdeepin/go-gir-generator
% cd $GOPATH/src/github.com/linuxdeepin/go-gir-generator
% make build copyfile
% mkdir -p $GOPATH/bin/
% cp -a out/gir-generator $GOPATH/bin/
% cp -a out/src/gir/ $GOPATH/src/
```

Now, you can generate Arrow bindings for Go:

```text
% git clone https://github.com/apache/arrow.git ~/arrow
% cd ~/arrow/c_glib/example/go
% make generate
```

Then you can build all example codes:

```text
% cd ~/arrow/c_glib/example/go
% make
% ./write-batch  # Write data in batch mode
% ./read-batch   # Read the written batch mode data
% ./write-stream # Write data in stream mode
% ./read-stream  # Read the written stream mode data
```

## Go example codes

Here are example codes in this directory:

  * `write-batch.go`: It shows how to write Arrow array to file in
    batch mode.

  * `read-batch.go`: It shows how to read Arrow array from file in
    batch mode.

  * `write-stream.go`: It shows how to write Arrow array to file in
    stream mode.

  * `read-stream.go`: It shows how to read Arrow array from file in
    stream mode.
