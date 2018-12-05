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

# Red Arrow CUDA - Apache Arrow CUDA Ruby

Red Arrow CUDA is the Ruby bindings of Apache Arrow CUDA. Red Arrow CUDA is based on GObject Introspection.

[Apache Arrow CUDA](https://arrow.apache.org/) is an in-memory columnar data store on GPU.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Arrow CUDA uses [Apache Arrow CUDA GLib](https://github.com/apache/arrow/tree/master/c_glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Apache Arrow CUDA.

Apache Arrow CUDA GLib is a C wrapper for [Apache Arrow CUDA C++](https://github.com/apache/arrow/tree/master/cpp). GObject Introspection can't use Apache Arrow CUDA C++ directly. Apache Arrow CUDA GLib is a bridge between Apache Arrow CUDA C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Arrow CUDA uses GObject Introspection via gobject-introspection gem.

## Install

Install Apache Arrow CUDA GLib before install Red Arrow CUDA. Use [packages.red-data-tools.org](https://github.com/red-data-tools/packages.red-data-tools.org) for installing Apache Arrow CUDA GLib.

Note that the Apache Arrow CUDA GLib packages are "unofficial". "Official" packages will be released in the future.

Install Red Arrow CUDA after you install Apache Arrow CUDA GLib:

```text
% gem install red-arrow-cuda
```

## Usage

```ruby
require "arrow-cuda"

manager = ArrowCUDA::DeviceManager.new
if manager.n_devices.zero?
  raise "No GPU is found"
end

context = manager[0]
buffer = ArrowCUDA::Buffer.new(context, 128)
ArrowCUDA::BufferOutputStream.open(buffer) do |stream|
  stream.write("Hello World")
end
puts buffer.copy_to_host(0, 11) # => "Hello World"
```
