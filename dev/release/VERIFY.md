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

We've provided a convenience script for verifying the C++, C#, C GLib, Go,
Java, JavaScript, Ruby and Python builds on Linux and macOS. Read the script
`verify-release-candidate.sh` for further information.

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

You can install them by the followings on CentOS 7:

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

### C++, C#, C GLib, Go, Java, JavaScript, Python, Ruby

Example scripts to install the dependencies to run the verification
script for verifying the source on Ubuntu 20.04, Rocky Linux 8 and
AlmaLinux 8 are in this folder and named `setup-ubuntu.sh` and
`setup-rhel-rebuilds.sh`. These can be adapted to different
situations. Go and JavaScript are installed by the verification
script in the testing environment. Verifying the apt and yum binaries
additionally requires installation of Docker.

When verifying the source, by default the verification script will try
to verify all implementations and bindings. Should one of the
verification tests fail, the script will exit before running the other
tests. It can be helpful to repeat the failed test to see if it will
complete, since failures can occur for problems such as slow or failed
download of a dependency from the internet. It is possible to run
specific verification tests by setting environment variables, for example

```console
% TEST_DEFAULT=0 TEST_SOURCE=1 dev/release/verify-release-candidate.sh 6.0.0 3
% TEST_DEFAULT=0 TEST_BINARIES=1 dev/release/verify-release-candidate.sh 6.0.0 3
% TEST_DEFAULT=0 TEST_GO=1 dev/release/verify-release-candidate.sh 6.0.0 3
% TEST_DEFAULT=0 TEST_YUM=1 dev/release/verify-release-candidate.sh 6.0.0 3
```

It is also possible to use
[Archery](https://arrow.apache.org/docs/developers/archery.html) to run
the verification process in a container, for example

```console
% archery docker run -e VERIFY_VERSION=6.0.1 -e VERIFY_RC=1 almalinux-verify-rc-source
% archery docker run -e VERIFY_VERSION=6.0.1 -e VERIFY_RC=1 ubuntu-verify-rc-source
```

To improve software quality, you are encouraged to verify
on a variety of platforms.
