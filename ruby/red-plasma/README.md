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

# Red Plasma - Plasma Ruby

Red Plasma is the Ruby bindings of Plasma. Red Plasma is based on GObject Introspection.

Plasma is an in-memory object store and cache for big data.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Plasma uses [Plasma GLib](https://github.com/apache/arrow/tree/master/c_glib/plasma-glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Plasma.

Plasma GLib is a C wrapper for [Plasma C++](https://github.com/apache/arrow/tree/master/cpp/plasma). GObject Introspection can't use Plasma C++ directly. Plasma GLib is a bridge between Plasma C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Plasma uses GObject Introspection via gobject-introspection gem.

## Install

Install Plasma GLib before install Red Plasma. Use [packages.red-data-tools.org](https://github.com/red-data-tools/packages.red-data-tools.org) for installing Plasma GLib.

Note that the Plasma GLib packages are "unofficial". "Official" packages will be released in the future.

Install Red Plasma after you install Plasma GLib:

```text
% gem install red-plasma
```

## Usage

Starting the Plasma store

```console
plasma_store_server -m 1000000000 -s /tmp/plasma
```

Creating a Plasma client

```ruby
require "plasma"

client = Plasma::Client.new("/tmp/plasma")
```
