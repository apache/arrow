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

Arrow GLib is a wrapper library for [Arrow
C++](https://github.com/apache/arrow/tree/main/cpp). Arrow GLib
provides C API.

Arrow GLib supports [GObject
Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection).
It means that you can create language bindings at runtime or compile
time.

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

We use Meson and Ninja as build tools. If you find problems when
installing please see [common build
problems](https://github.com/apache/arrow/blob/main/c_glib/README.md#common-build-problems).

### Packages

See [install document](https://arrow.apache.org/install/) for details.

### How to build by users

Arrow GLib users should use released source archive to build Arrow
GLib (replace the version number in the following commands with the
one you use):

```console
$ wget 'https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-12.0.0/apache-arrow-12.0.0.tar.gz'
$ tar xf apache-arrow-12.0.0.tar.gz
$ cd apache-arrow-12.0.0
```

You need to build and install Arrow C++ before you build and install
Arrow GLib. See Arrow C++ document about how to install Arrow C++.

If you use macOS with [Homebrew](https://brew.sh/), you must install
required packages.

macOS:

```console
$ brew bundle
$ meson setup c_glib.build c_glib --buildtype=release
$ meson compile -C c_glib.build
$ sudo meson install -C c_glib.build
```

Others:

```console
$ meson setup c_glib.build c_glib --buildtype=release
$ meson compile -C c_glib.build
$ sudo meson install -C c_glib.build
```

### How to build by developers

You need to install Arrow C++ before you install Arrow GLib. See Arrow
C++ document about how to install Arrow C++.

You need [GTK-Doc](https://www.gtk.org/gtk-doc/) and
[GObject Introspection](https://wiki.gnome.org/Projects/GObjectIntrospection)
to build Arrow GLib. You can install them by the followings:

On Debian GNU/Linux or Ubuntu:

```console
$ sudo apt install -y -V gtk-doc-tools libgirepository1.0-dev meson ninja-build
```

On CentOS 7:

```console
$ sudo yum install -y gtk-doc gobject-introspection-devel ninja-build
$ sudo pip3 install meson
```

On CentOS 8 or later:

```console
$ sudo dnf install -y --enablerepo=powertools gtk-doc gobject-introspection-devel ninja-build
$ sudo pip3 install meson
```

On macOS with [Homebrew](https://brew.sh/):

```console
$ brew bundle
```

You can build and install Arrow GLib by the followings:

macOS:

```console
$ XML_CATALOG_FILES=$(brew --prefix)/etc/xml/catalog
$ meson setup c_glib.build c_glib -Dgtk_doc=true
$ meson compile -C c_glib.build
$ sudo meson install -C c_glib.build
```

Others:

```console
$ meson c_glib.build c_glib -Dgtk_doc=true
$ meson compile -C c_glib.build
$ sudo meson install -C c_glib.build
```

## Usage

You can use Arrow GLib with C or other languages. If you use Arrow
GLib with C, you use C API. If you use Arrow GLib with other
languages, you use GObject Introspection based bindings.

### C

You can find API reference in the
`/usr/local/share/gtk-doc/html/arrow-glib/` directory. If you specify
`--prefix` to `meson`, the directory will be different.

You can find example codes in the `example/` directory.

### Language bindings

You can use Arrow GLib with non-C languages with GObject Introspection
based bindings. Here are languages that support GObject Introspection:

  * Ruby: [red-arrow gem](https://rubygems.org/gems/red-arrow) should be used.
    * Examples: https://github.com/red-data-tools/red-arrow/tree/master/example

  * Python: [PyGObject](https://wiki.gnome.org/Projects/PyGObject) should be used. (Note that you should prefer PyArrow over Arrow GLib.)

  * Lua: [LGI](https://github.com/pavouk/lgi) should be used.
    * Examples: `example/lua/` directory.

  * Go: [Go-gir-generator](https://github.com/linuxdeepin/go-gir-generator) should be used. (Note that you should use Apache Arrow for Go than Arrow GLib.)

See also
[Projects/GObjectIntrospection/Users - GNOME Wiki!](https://wiki.gnome.org/Projects/GObjectIntrospection/Users)
for other languages.

## How to run test

Arrow GLib has unit tests. You can confirm that you install Arrow
GLib correctly by running unit tests.

You need to install the followings to run unit tests:

  * [Ruby](https://www.ruby-lang.org/)
  * [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection)
  * [test-unit gem](https://rubygems.org/gems/test-unit)

You can install them by the followings:

On Debian GNU/Linux or Ubuntu:

```console
$ sudo apt install -y -V ruby-dev
$ sudo gem install bundler
$ (cd c_glib && bundle install)
```

On CentOS 7 or later:

```console
$ sudo yum install -y git
$ git clone https://github.com/sstephenson/rbenv.git ~/.rbenv
$ git clone https://github.com/sstephenson/ruby-build.git ~/.rbenv/plugins/ruby-build
$ echo 'export PATH="$HOME/.rbenv/bin:$PATH"' >> ~/.bash_profile
$ echo 'eval "$(rbenv init -)"' >> ~/.bash_profile
$ exec ${SHELL} --login
$ sudo yum install -y gcc make patch openssl-devel readline-devel zlib-devel
$ latest_ruby_version=$(rbenv install --list 2>&1 | grep '^[0-9]' | tail -n1)
$ rbenv install ${latest_ruby_version}
$ rbenv global ${latest_ruby_version}
$ gem install bundler
$ (cd c_glib && bundle install)
```

On macOS with [Homebrew](https://brew.sh/):

```console
$ (cd c_glib && bundle install)
```

Now, you can run unit tests by the followings:

```console
$ cd c_glib.build
$ bundle exec ../c_glib/test/run-test.sh
```

## Common build problems

### build failed - /usr/bin/ld: cannot find -larrow

Arrow C++ must be installed to build Arrow GLib. Run `make install` on
Arrow C++ build directory. In addition, on linux, you may need to run
`sudo ldconfig`.

### build failed - unable to load http://docbook.sourceforge.net/release/xsl/current/html/chunk.xsl

You need to set the following environment variable on macOS:

```console
$ export XML_CATALOG_FILES="$(brew --prefix)/etc/xml/catalog"
```

### build failed - Symbol not found, referenced from `libsource-highlight.4.dylib`

You may get the following error on macOS:


```text
dyld: Symbol not found: __ZN5boost16re_detail_10650112perl_matcherIPKcNSt3__19allocatorINS_9sub_matchIS3_EEEENS_12regex_traitsIcNS_16cpp_regex_traitsIcEEEEE14construct_initERKNS_11basic_regexIcSC_EENS_15regex_constants12_match_flagsE
  Referenced from: /usr/local/Cellar/source-highlight/3.1.8_7/lib/libsource-highlight.4.dylib
  Expected in: flat namespace
 in /usr/local/Cellar/source-highlight/3.1.8_7/lib/libsource-highlight.4.dylib
```

To fix this error, you need to upgrade `source-highlight`:

```console
$ brew upgrade source-highlight
```

### test failed - Failed to load shared library '...' referenced by the typelib: dlopen(...): dependent dylib '@rpath/...' not found for '...'. relative file paths not allowed '@rpath/...'

You may get the following error on macOS by running test:

```text
(NULL)-WARNING **: Failed to load shared library '/usr/local/lib/libparquet-glib.400.dylib' referenced by the typelib: dlopen(/usr/local/lib/libparquet-glib.400.dylib, 0x0009): dependent dylib '@rpath/libparquet.400.dylib' not found for '/usr/local/lib/libparquet-glib.400.dylib'. relative file paths not allowed '@rpath/libparquet.400.dylib'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/loader.rb:215:in `load_object_info'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/loader.rb:68:in `load_info'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/loader.rb:43:in `block in load'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/repository.rb:34:in `block (2 levels) in each'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/repository.rb:33:in `times'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/repository.rb:33:in `block in each'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/repository.rb:32:in `each'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/repository.rb:32:in `each'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection/loader.rb:42:in `load'
        from /Library/Ruby/Gems/2.6.0/gems/gobject-introspection-3.4.3/lib/gobject-introspection.rb:44:in `load'
        from /Users/karlkatzen/Documents/code/arrow-dev/arrow/c_glib/test/run-test.rb:60:in `<main>'
Traceback (most recent call last):
        17: from /Users/karlkatzen/Documents/code/arrow-dev/arrow/c_glib/test/run-test.rb:80:in `<main>'
        16: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/autorunner.rb:66:in `run'
        15: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/autorunner.rb:434:in `run'
        14: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/autorunner.rb:106:in `block in <class:AutoRunner>'
        13: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:38:in `collect'
        12: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:136:in `add_load_path'
        11: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:43:in `block in collect'
        10: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:43:in `each'
         9: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:46:in `block (2 levels) in collect'
         8: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:85:in `collect_recursive'
         7: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:85:in `each'
         6: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:87:in `block in collect_recursive'
         5: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:112:in `collect_file'
         4: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:136:in `add_load_path'
         3: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:114:in `block in collect_file'
         2: from /Library/Ruby/Gems/2.6.0/gems/test-unit-3.4.0/lib/test/unit/collector/load.rb:114:in `require'
         1: from /Users/karlkatzen/Documents/code/arrow-dev/arrow/c_glib/test/test-extension-data-type.rb:18:in `<top (required)>'
/Users/karlkatzen/Documents/code/arrow-dev/arrow/c_glib/test/test-extension-data-type.rb:19:in `<class:TestExtensionDataType>': uninitialized constant Arrow::ExtensionArray (NameError)
```

You can't use `@rpath` in Arrow C++. To fix this error, you need to
build Arrow C++ with `-DARROW_INSTALL_NAME_RPATH=OFF`:

```console
$ cmake -S cpp -B cpp.build -DARROW_INSTALL_NAME_RPATH=OFF ...
$ cmake --build cpp.build
$ sudo cmake --build cpp.build --target install
```
