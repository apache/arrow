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

# Linux packages for Apache Arrow C++ and GLib

## Requirements

  * Ruby
  * Docker
  * Tools to build tar.gz for Apache Arrow C++ and GLib

## How to build .deb packages for all supported platforms

```bash
cd dev/tasks/linux-packages/apache-arrow
rake version:update
rake apt:build
```

## How to build only specific .deb packages for supported platforms

The following command line shows all supported platforms, bear in mind
to execute this command from your root `arrow` clone folder:

```bash
for x in dev/tasks/linux-packages/apache-arrow/apt/{debian,ubuntu}*; do basename $x; done
```

You can specify target platforms by setting `APT_TARGETS`:

```bash
cd dev/tasks/linux-packages/apache-arrow
rake version:update
rake apt:build APT_TARGETS=debian-bookworm,ubuntu-noble
```

## How to debug .deb packages build

You can use `apt:build:console` task to debug .deb packages build:

```bash
cd dev/tasks/linux-packages/apache-arrow
rake version:update
rake apt:build:console APT_TARGETS=debian-bookworm
```

It will show a Bash prompt. You can start .deb build by `/host/build.sh`:

```console
host$ rake apt:build:console APT_TARGETS=debian-bookworm
container$ /host/build.sh
```

You can keep the Bash session even when the .deb build failed. You can
debug in the Bash session.

## How to build .rpm packages for all supported platforms

```bash
cd dev/tasks/linux-packages/apache-arrow
rake yum:build
```

## How to build only specific .rpm packages for supported platforms

The following command line shows all supported platforms, bear in mind
to execute this command from your root `arrow` clone folder:

```bash
for x in dev/tasks/linux-packages/apache-arrow/yum/{alma,amazon,centos}*; do basename $x; done
```

You can specify target platforms by setting `YUM_TARGETS`:

```bash
cd dev/tasks/linux-packages/apache-arrow
rake yum:build YUM_TARGETS=almalinux-9,amazon-linux-2023
```

## How to debug .rpm packages build

You can use `yum:build:console` task to debug .rpm packages build:

```bash
cd dev/tasks/linux-packages/apache-arrow
rake yum:build:console YUM_TARGETS=almalinux-9
```

It will show a Bash prompt. You can start .rpm build by `/host/build.sh`:

```console
host$ rake yum:build:console YUM_TARGETS=almalinux-9
container$ /host/build.sh
```

You can keep the Bash session even when the .rpm build failed. You can
debug in the Bash session.
