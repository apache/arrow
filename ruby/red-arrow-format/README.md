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

# Red Arrow Format - Apache Arrow Format Ruby

Red Arrow Format is the pure Ruby Apache Arrow format serializer and deserializer implementation. This provides only serialize/deserialize features. If you want to process Apache Arrow data not only serialize/desrialize Apache Arrow data, you should use Red Arrow not Red Arrow Format.

[Apache Arrow](https://arrow.apache.org/) is an in-memory columnar data store. It's used by many products for data analytics.

## Install

If you want to install Red Arrow Format by Bundler, you can add the followings to your `Gemfile`:

```ruby
gem "red-arrow-format"
```

If you want to install Red Arrow Format by RubyGems, you can use the following command line:

```console
$ gem install red-arrow-format
```

## Usage

```ruby
require "arrow-format"

File.open("/dev/shm/data.arrow", "rb") do |input|
  reader = ArrowFormat::FileReader.new(input)
  reader.each do |record_batch|
    # Use record_batch
  end
end
```

## Development

You can run tests by the following command lines:

```console
$ cd ruby/red-arrow-format
$ bundle install
$ bundle exec rake test
```
