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

# Red Arrow Flight SQL - Apache Arrow Flight SQL Ruby

Red Arrow Flight SQL is the Ruby bindings of Apache Arrow Flight SQL. Red Arrow Flight SQL is based on GObject Introspection.

[Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) is one of Apache Arrow components to interact with SQL databases using the Apache Arrow in-memory format and [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Arrow Flight SQL uses [Apache Arrow Flight SQL GLib](https://github.com/apache/arrow/tree/main/c_glib/arrow-flight-sql) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Apache Arrow Flight SQL.

Apache Arrow Flight SQL GLib is a C wrapper for [Apache Arrow Flight SQL C++](https://github.com/apache/arrow/tree/main/cpp/src/arrow/flight/sql). GObject Introspection can't use Apache Arrow Flight SQL C++ directly. Apache Arrow Flight SQL GLib is a bridge between Apache Arrow Flight SQL C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Arrow Flight SQL uses GObject Introspection via gobject-introspection gem.

## Install

Install Apache Arrow Flight SQL GLib before install Red Arrow Flight SQL. See [Apache Arrow install document](https://arrow.apache.org/install/) for details.

Install Red Arrow Flight SQL after you install Apache Arrow Flight GLib:

```console
$ gem install red-arrow-flight-sql
```

## Usage

```ruby
require "arrow-flight-sql"

# TODO
```
