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

# Red Arrow Flight - Apache Arrow Flight Ruby

Red Arrow Flight is the Ruby bindings of Apache Arrow Flight. Red Arrow Flight is based on GObject Introspection.

[Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) is one of Apache Arrow components to read and write semantic flights stored in different locations and formats.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Arrow Flight uses [Apache Arrow Flight GLib](https://github.com/apache/arrow/tree/main/c_glib/arrow-flight-glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Apache Arrow Flight.

Apache Arrow Flight GLib is a C wrapper for [Apache Arrow Flight C++](https://github.com/apache/arrow/tree/main/cpp/src/arrow/flight). GObject Introspection can't use Apache Arrow Flight C++ directly. Apache Arrow Flight GLib is a bridge between Apache Arrow Flight C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Arrow Flight uses GObject Introspection via gobject-introspection gem.

## Install

Install Apache Arrow Flight GLib before install Red Arrow Flight. See [Apache Arrow install document](https://arrow.apache.org/install/) for details.

Install Red Arrow Flight after you install Apache Arrow Flight GLib:

```console
$ gem install red-arrow-flight
```

## Usage

```ruby
require "arrow-flight"

# TODO
```
