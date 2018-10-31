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

# Red Parquet - Apache Parquet Ruby

Red Parquet is the Ruby bindings of Apache Parquet. Red Parquet is based on GObject Introspection.

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Parquet uses [Apache Parquet GLib](https://github.com/apache/arrow/tree/master/c_glib/parquet-glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Apache Parquet.

Apache Parquet GLib is a C wrapper for [Apache Parquet C++](https://github.com/apache/arrow/tree/master/cpp/parquet). GObject Introspection can't use Apache Parquet C++ directly. Apache Parquet GLib is a bridge between Apache Parquet C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Parquet uses GObject Introspection via gobject-introspection gem.

## Install

Install Apache Parquet GLib before install Red Parquet. Use [packages.red-data-tools.org](https://github.com/red-data-tools/packages.red-data-tools.org) for installing Apache Parquet GLib.

Note that the Apache Parquet GLib packages are "unofficial". "Official" packages will be released in the future.

Install Red Parquet after you install Apache Parquet GLib:

```text
% gem install red-parquet
```

## Usage

```ruby
require "parquet"

table = Arrow::Table.load("/dev/shm/data.parquet")
# Process data in table
table.save("/dev/shm/data-processed.parquet")
```
