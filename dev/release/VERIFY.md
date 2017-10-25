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

# Verifying Arrow releases

## Windows

We've provided a convenience script for verifying the C++ and Python builds on
Windows. Read the comments in `verify-release-candidate.bat` for instructions.

## Linux and macOS

We've provided a convenience script for verifying the C++, Python, C
GLib, Java and JavaScript builds on Linux and macOS. Read the comments in
`verify-release-candidate.sh` for instructions.

### C GLib

You need the followings to verify C GLib build:

  * GLib
  * GObject Introspection
  * Ruby (not EOL-ed version is required)
  * gobject-introspection gem
  * test-unit gem

You can install them by the followings on Debian GNU/Linux and Ubuntu:

```console
% sudo apt install -y -V libgirepository1.0-dev ruby-dev
% sudo gem install gobject-introspection test-unit
```

You can install them by the followings on CentOS:

```console
% sudo yum install -y gobject-introspection-devel
% git clone https://github.com/sstephenson/rbenv.git ~/.rbenv
% git clone https://github.com/sstephenson/ruby-build.git ~/.rbenv/plugins/ruby-build
% echo 'export PATH="$HOME/.rbenv/bin:$PATH"' >> ~/.bash_profile
% echo 'eval "$(rbenv init -)"' >> ~/.bash_profile
% exec ${SHELL} --login
% sudo yum install -y gcc make patch openssl-devel readline-devel zlib-devel
% rbenv install 2.4.2
% rbenv global 2.4.2
% gem install gobject-introspection test-unit
```

You can install them by the followings on macOS:

```console
% brew install -y gobject-introspection
% gem install gobject-introspection test-unit
```

You need to set `PKG_CONFIG_PATH` to find libffi on macOS:

```console
% export PKG_CONFIG_PATH=$(brew --prefix libffi)/lib/pkgconfig:$PKG_CONFIG_PATH
```
