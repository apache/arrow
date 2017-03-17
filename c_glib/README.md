<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Arrow GLib

Arrow GLib is a wrapper library for Arrow C++. Arrow GLib provides C
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

### Package

TODO

### Build

You need to install Arrow C++ before you install Arrow GLib. See Arrow
C++ document about how to install Arrow C++.

You need [GTK-Doc](https://www.gtk.org/gtk-doc/) and
[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection)
to build Arrow GLib. You can install them by the followings:

On Debian GNU/Linux or Ubuntu:

```text
% sudo apt install -y -V gtk-doc-tools libgirepository1.0-dev
```

On CentOS 7 or later:

```text
% sudo yum install -y gtk-doc gobject-introspection-devel
```

On macOS with [Homebrew](https://brew.sh/):

```text
% brew install -y gtk-doc gobject-introspection
```

Now, you can build Arrow GLib:

```text
% cd glib
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

  * Python: [PyGObject](https://wiki.gnome.org/Projects/PyGObject) should be used. (Note that you should use PyArrow than Arrow GLib.)

  * Lua: [LGI](https://github.com/pavouk/lgi) should be used.

  * Go: [Go-gir-generator](https://github.com/linuxdeepin/go-gir-generator) should be used.

See also
[Projects/GObjectIntrospection/Users - GNOME Wiki!](https://wiki.gnome.org/Projects/GObjectIntrospection/Users)
for other languages.
