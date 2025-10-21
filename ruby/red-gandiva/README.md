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

# Red Gandiva - Gandiva Ruby

Red Gandiva is the Ruby bindings of Gandiva. Red Gandiva is based on GObject Introspection.

Gandiva is a toolset for compiling and evaluating expressions on Arrow data.

[GObject Introspection](https://wiki.gnome.org/action/show/Projects/GObjectIntrospection) is a middleware for language bindings of C library. GObject Introspection can generate language bindings automatically at runtime.

Red Gandiva uses [Gandiva GLib](https://github.com/apache/arrow/tree/main/c_glib/gandiva-glib) and [gobject-introspection gem](https://rubygems.org/gems/gobject-introspection) to generate Ruby bindings of Gandiva.

Gandiva GLib is a C wrapper for [Gandiva C++](https://github.com/apache/arrow/tree/main/cpp/gandiva). GObject Introspection can't use Gandiva C++ directly. Gandiva GLib is a bridge between Gandiva C++ and GObject Introspection.

gobject-introspection gem is a Ruby bindings of GObject Introspection. Red Gandiva uses GObject Introspection via gobject-introspection gem.

## Install

Install Gandiva GLib before install Red Gandiva. See [Apache Arrow install document](https://arrow.apache.org/install/) for details.

Install Red Gandiva after you install Gandiva GLib:

```text
% gem install red-gandiva
```

## Usage

```ruby
require "gandiva"

table = Arrow::Table.new(:field1 => Arrow::Int32Array.new([1, 2, 3, 4]),
                         :field2 => Arrow::Int32Array.new([11, 13, 15, 17]))
schema = table.schema

expression1 = schema.build_expression do |record|
  record.field1 + record.field2
end

expression2 = schema.build_expression do |record, context|
  context.if(record.field1 > record.field2)
    .then(record.field1 / record.field2)
    .else(record.field1)
end

projector = Gandiva::Projector.new(schema, [expression1, expression2])
table.each_record_batch do |record_batch|
  outputs = projector.evaluate(record_batch)
  puts outputs.collect(&:values)
end
```
