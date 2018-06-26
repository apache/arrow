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

# Arrow GLib

Arrow GLib is a wrapper library for [Arrow C++](https://github.com/apache/arrow/tree/master/cpp). Arrow GLib provides C
API.

Arrow GLib supports
[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection).
It means that you can create language bindings at runtime or compile time.

For example, you can use Apache Arrow from Ruby by Arrow GLib and
[gobject-introspection gem](https://rubygems.org/gems/gobject-introspection)
with the following code:

```ruby
# Generate bindings at runtime
require "gi"
Arrow = GI.load("Arrow")

# Now, you can access arrow::BooleanArray in Arrow C++ by
# Arrow::BooleanArray
p Arrow::BooleanArray
```

In Ruby case, you should use
[red-arrow gem](https://rubygems.org/gems/red-arrow). It's based on
gobject-introspection gem. It adds many convenient features to raw
gobject-introspection gem based bindings.

## Install

You can use packages or build by yourself to install Arrow GLib. It's
recommended that you use packages.

Note that the packages are "unofficial". "Official" packages will be
released in the future.

If you find problems when installing please see [common build problems](https://github.com/apache/arrow/blob/master/c_glib/README.md#common-build-problems).

### Package

See [install document](../site/install.md) for details.

### How to build by users

Arrow GLib users should use released source archive to build Arrow
GLib (replace the version number in the following commands with the one you use):

```text
% wget https://archive.apache.org/dist/arrow/arrow-0.3.0/apache-arrow-0.3.0.tar.gz
% tar xf apache-arrow-0.3.0.tar.gz
% cd apache-arrow-0.3.0
```

You need to build and install Arrow C++ before you build and install
Arrow GLib. See Arrow C++ document about how to install Arrow C++.

You can build and install Arrow GLib after you install Arrow C++.

If you use macOS with [Homebrew](https://brew.sh/), you must install `gobject-introspection` and set `PKG_CONFIG_PATH` before build Arrow GLib:

```text
% cd c_glib
% brew install -y gobject-introspection
% ./configure PKG_CONFIG_PATH=$(brew --prefix libffi)/lib/pkgconfig:$PKG_CONFIG_PATH
% make
% sudo make install
```

Others:

```text
% cd c_glib
% ./configure
% make
% sudo make install
```

### How to build by developers

You need to install Arrow C++ before you install Arrow GLib. See Arrow
C++ document about how to install Arrow C++.

You need [GTK-Doc](https://www.gtk.org/gtk-doc/) and
[GObject Introspection](https://wiki.gnome.org/Projects/GObjectIntrospection)
to build Arrow GLib. You can install them by the followings:

On Debian GNU/Linux or Ubuntu:

```text
% sudo apt install -y -V gtk-doc-tools autoconf-archive libgirepository1.0-dev
```

On CentOS 7 or later:

```text
% sudo yum install -y gtk-doc gobject-introspection-devel
```

On macOS with [Homebrew](https://brew.sh/):

```text
% brew bundle
```

Now, you can build Arrow GLib:

```text
% cd c_glib
% ./autogen.sh
% ./configure --enable-gtk-doc
% make
% sudo make install
```

## Usage

You can use Arrow GLib with C or other languages. If you use Arrow
GLib with C, you use C API. If you use Arrow GLib with other
languages, you use GObject Introspection based bindings.

### C

You can find API reference in the
`/usr/local/share/gtk-doc/html/arrow-glib/` directory. If you specify
`--prefix` to `configure`, the directory will be different.

You can find example codes in the `example/` directory.

### Language bindings

You can use Arrow GLib with non C languages with GObject Introspection
based bindings. Here are languages that support GObject Introspection:

  * Ruby: [red-arrow gem](https://rubygems.org/gems/red-arrow) should be used.
    * Examples: https://github.com/red-data-tools/red-arrow/tree/master/example

  * Python: [PyGObject](https://wiki.gnome.org/Projects/PyGObject) should be used. (Note that you should use PyArrow than Arrow GLib.)

  * Lua: [LGI](https://github.com/pavouk/lgi) should be used.
    * Examples: `example/lua/` directory.

  * Go: [Go-gir-generator](https://github.com/linuxdeepin/go-gir-generator) should be used. (Note that you should use Apache Arrow for Go than Arrow GLib.)

See also
[Projects/GObjectIntrospection/Users - GNOME Wiki!](https://wiki.gnome.org/Projects/GObjectIntrospection/Users)
for other languages.

## How to run test

Arrow GLib has unit tests. You can confirm that you install Apache
GLib correctly by running unit tests.

You need to install the followings to run unit tests:

  * [Ruby](https://www.ruby-lang.org/)
  * [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection)
  * [test-unit gem](https://rubygems.org/gems/test-unit)

You can install them by the followings:

On Debian GNU/Linux or Ubuntu:

```text
% sudo apt install -y -V ruby-dev
% sudo gem install gobject-introspection test-unit
```

On CentOS 7 or later:

```text
% sudo yum install -y git
% git clone https://github.com/sstephenson/rbenv.git ~/.rbenv
% git clone https://github.com/sstephenson/ruby-build.git ~/.rbenv/plugins/ruby-build
% echo 'export PATH="$HOME/.rbenv/bin:$PATH"' >> ~/.bash_profile
% echo 'eval "$(rbenv init -)"' >> ~/.bash_profile
% exec ${SHELL} --login
% sudo yum install -y gcc make patch openssl-devel readline-devel zlib-devel
% rbenv install 2.4.1
% rbenv global 2.4.1
% gem install gobject-introspection test-unit
```

On macOS with [Homebrew](https://brew.sh/):

```text
% gem install gobject-introspection test-unit
```

Now, you can run unit tests by the followings:

```text
% cd c_glib
% test/run-test.sh
```

## Common build problems

### configure failed - `AX_CXX_COMPILE_STDCXX_11(ext, mandatory)'

* Check whether `autoconf-archive` is installed.
* [macOS] `autoconf-archive` must be linked, but may not be linked. You can check it by running `brew install autoconf-archive` again. If it's not linked, it will show a warning message like:

```console
% brew install autoconf-archive
Warning: autoconf-archive 2017.03.21 is already installed, it's just not linked.
You can use `brew link autoconf-archive` to link this version.
```

In this case, you need to run `brew link autoconf-archive`. It may fail with the following message if you have install conflicted packages (e.g. `gnome-common`).

```console
% brew link autoconf-archive
Linking /usr/local/Cellar/autoconf-archive/2017.03.21...
Error: Could not symlink share/aclocal/ax_check_enable_debug.m4
Target /usr/local/share/aclocal/ax_check_enable_debug.m4
is a symlink belonging to gnome-common. You can unlink it:
  brew unlink gnome-common
```

You need to run `brew unlink <pkgname>`, then run `brew link autoconf-archive` again.

After installing/linking `autoconf-archive`, run `./autogen.sh` again.

### [macOS] configure failed - gobject-introspection-1.0 is not installed

gobject-introspection requires libffi, and it's automatically installed with gobject-introspection. However it can't be found because it's [keg-only](https://docs.brew.sh/FAQ.html#what-does-keg-only-mean). You need to set `PKG_CONFIG_PATH` when executing configure.

```console
% ./configure PKG_CONFIG_PATH=$(brew --prefix libffi)/lib/pkgconfig
```

### build failed - /usr/bin/ld: cannot find -larrow

Arrow C++ must be installed to build Arrow GLib. Run `make install` on Arrow C++ build directory. In addtion, on linux, you may need to run `sudo ldconfig`.

### build failed - unable to load http://docbook.sourceforge.net/release/xsl/current/html/chunk.xsl

On macOS you may need to set the following environment variable:

```console
% export XML_CATALOG_FILES="/usr/local/etc/xml/catalog"
```

### build failed - Symbol not found, referenced from `libsource-highlight.4.dylib`

On macOS if you see the following error you may need to upgrade `source-highlight`

```console
dyld: Symbol not found: __ZN5boost16re_detail_10650112perl_matcherIPKcNSt3__19allocatorINS_9sub_matchIS3_EEEENS_12regex_traitsIcNS_16cpp_regex_traitsIcEEEEE14construct_initERKNS_11basic_regexIcSC_EENS_15regex_constants12_match_flagsE
  Referenced from: /usr/local/Cellar/source-highlight/3.1.8_7/lib/libsource-highlight.4.dylib
  Expected in: flat namespace
 in /usr/local/Cellar/source-highlight/3.1.8_7/lib/libsource-highlight.4.dylib
```

To fix do:

```console
% brew upgrade source-highlight
```
