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

# Red Arrow Dataset - Apache Arrow Dataset Ruby

Red Arrow Dataset is the Ruby bindings of Apache Arrow Dataset. Red Arrow Dataset is based on GObject Introspection.

[Apache Arrow Dataset](https://arrow.apache.org/) is one of Apache Arrow components to read and write semantic datasets stored in different locations and formats.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Arrow Dataset uses [Apache Arrow Dataset GLib](https://github.com/apache/arrow/tree/main/c_glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Apache Arrow Dataset.

Apache Arrow Dataset GLib is a C wrapper for [Apache Arrow Dataset C++](https://github.com/apache/arrow/tree/main/cpp). GObject Introspection can't use Apache Arrow Dataset C++ directly. Apache Arrow Dataset GLib is a bridge between Apache Arrow Dataset C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Arrow Dataset uses GObject Introspection via gobject-introspection gem.

## Install

Install Apache Arrow Dataset GLib before install Red Arrow Dataset. Install Apache Arrow GLib before install Red Arrow. See [Apache Arrow install document](https://arrow.apache.org/install/) for details.

Install Red Arrow Dataset after you install Apache Arrow Dataset GLib:

```console
$ gem install red-arrow-dataset
```

## Usage

```ruby
require "arrow-dataset"

# TODO
```
