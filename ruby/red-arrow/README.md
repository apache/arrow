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

# Red Arrow - Apache Arrow Ruby

Red Arrow is the Ruby bindings of Apache Arrow. Red Arrow is based on GObject Introspection.

[Apache Arrow](https://arrow.apache.org/) is an in-memory columnar data store. It's used by many products for data analytics.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Arrow uses [Apache Arrow GLib](https://github.com/apache/arrow/tree/main/c_glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Apache Arrow.

Apache Arrow GLib is a C wrapper for [Apache Arrow C++](https://github.com/apache/arrow/tree/main/cpp). GObject Introspection can't use Apache Arrow C++ directly. Apache Arrow GLib is a bridge between Apache Arrow C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Arrow uses GObject Introspection via gobject-introspection gem.

## Install

Install Apache Arrow GLib before install Red Arrow. See [Apache Arrow install document](https://arrow.apache.org/install/) for details.

Install Red Arrow after you install Apache Arrow GLib:

```console
% gem install red-arrow
```

## Usage

```ruby
require "arrow"

table = Arrow::Table.load("/dev/shm/data.arrow")
# Process data in table
table.save("/dev/shm/data-processed.arrow")
```

## Development

Note that you need to install Apache Arrow C++/GLib at master before preparing Red Arrow. See also:

  * For Apache Arrow C++: https://arrow.apache.org/docs/developers/cpp/building.html
  * For Apache Arrow GLib: https://github.com/apache/arrow/blob/main/c_glib/README.md

```console
$ cd ruby/red-arrow
$ bundle install
$ bundle exec rake test
```

### For macOS with Homebrew

```console
$ cd ruby/red-arrow
$ bundle install
$ brew install apache-arrow --head
$ brew install apache-arrow-glib --head
$ bundle exec rake test
```