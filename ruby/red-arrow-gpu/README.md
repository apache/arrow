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

# Red Arrow GPU - Apache Arrow GPU Ruby

Red Arrow GPU is the Ruby bindings of Apache Arrow GPU. Red Arrow GPU is based on GObject Introspection.

[Apache Arrow GPU](https://arrow.apache.org/) is an in-memory columnar data store on GPU.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Arrow GPU uses [Apache Arrow GPU GLib](https://github.com/apache/arrow/tree/master/c_glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Apache Arrow GPU.

Apache Arrow GPU GLib is a C wrapper for [Apache Arrow GPU C++](https://github.com/apache/arrow/tree/master/cpp). GObject Introspection can't use Apache Arrow GPU C++ directly. Apache Arrow GPU GLib is a bridge between Apache Arrow GPU C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Arrow GPU uses GObject Introspection via gobject-introspection gem.

## Install

Install Apache Arrow GPU GLib before install Red Arrow GPU. Use [packages.red-data-tools.org](https://github.com/red-data-tools/packages.red-data-tools.org) for installing Apache Arrow GPU GLib.

Install Red Arrow GPU after you install Apache Arrow GPU GLib:

```text
% gem install red-arrow-gpu
```

## Usage

```ruby
require "arrow-gpu"

manager = ArrowGPU::CUDADeviceManager.new
if manager.n_devices.zero?
  raise "No GPU is found"
end

context = manager[0]
buffer = ArrowGPU::CUDABuffer.new(context, 128)
ArrowGPU::CUDABufferOutputStream.open(buffer) do |stream|
  stream.write("Hello World")
end
puts buffer.copy_to_host(0, 11) # => "Hello World"
```
