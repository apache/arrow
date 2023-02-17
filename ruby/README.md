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

# Apache Arrow Ruby

Here are the official Ruby bindings for Apache Arrow.

[Red Arrow](https://github.com/apache/arrow/tree/main/ruby/red-arrow) is the base Apache Arrow bindings.

[Red Arrow CUDA](https://github.com/apache/arrow/tree/main/ruby/red-arrow-cuda) is the Apache Arrow bindings of CUDA part.

[Red Arrow Dataset](https://github.com/apache/arrow/tree/main/ruby/red-arrow-dataset) is the Apache Arrow Dataset bindings.

[Red Gandiva](https://github.com/apache/arrow/tree/main/ruby/red-gandiva) is the Gandiva bindings.

[Red Plasma](https://github.com/apache/arrow/tree/main/ruby/red-plasma) is the Plasma bindings. (This is deprecated since 10.0.0. This will be removed from 12.0.0 or so.)

[Red Parquet](https://github.com/apache/arrow/tree/main/ruby/red-parquet) is the Parquet bindings.


## Cookbook

### Getting Started

```shell
gem install red-arrow
gem install red-parquet # for parquet support
gem install red-arrow-dataset # reading from s3 / folders
```

### Create table
#### From file
```ruby
require 'arrow'
require 'parquet'

table = Arrow::Table.load('data.arrow')
table = Arrow::Table.load('data.csv', format: :csv)
table = Arrow::Table.load('data.parquet', format: :parquet)
```
#### From Ruby hash
Types will be detected automatically
```ruby
table = Arrow::Table.new('name' => ['Tom', 'Max'], 'age' => [22, 23])
```
#### From String
Suppose you have your data available via HTTP. Let's connect to demo ClickHouse DB. See https://play.clickhouse.com/ for details
```ruby
require 'net/http'

params = {
  query: "SELECT WatchID as watch FROM hits LIMIT 10 FORMAT Arrow",
  user: "play",
  password: "",
  database: "default"
}
uri = URI('https://play.clickhouse.com:443/')
uri.query = URI.encode_www_form(params)
resp = Net::HTTP.get(uri)
table = Arrow::Table.load(Arrow::Buffer.new(resp))
```
#### From S3
```ruby
require 'arrow-dataset'

s3_uri = URI('s3://bucket/public.csv')
Arrow::Table.load(s3_uri)
```
For private access you can pass access_key and secret_key in following way:
```ruby
require 'cgi/util'

s3_uri = URI("s3://#{CGI.escape(access_key)}:#{CGI.escape(secret_key)}@bucket/private.parquet")
Arrow::Table.load(s3_uri)
```
#### From multiple files in folder
```ruby
require 'arrow-dataset'

Arrow::Table.load(URI("file:///your/folder/"), format: :parquet)
```

### Filtering
Uses concept of slicers in Arrow
```ruby
table = Arrow::Table.new(
  'name' => ['Tom', 'Max', 'Kate'],
  'age' => [22, 23, 19]
)
table.slice { |slicer| slicer['age'] > 19 }
# => #<Arrow::Table:0x7fa38838c448 ptr=0x7fa3ad269f40>
#   name	age
# 0	Tom 	 22
# 1	Max 	 23

table.slice { |slicer| slicer['age'].in?(19..22) }
# => #<Arrow::Table:0x7fa3881cf998 ptr=0x7fa3a4bb5f30>
#   name	age
# 0	Tom 	 22
# 1	Kate	 19
```
Multiple slice conditions can be joined using and(`&`) / or (`|`) / xor(`^`) logical operations
```ruby
table.slice { |slicer| (slicer['age'] > 19) & (slicer['age'] < 23) }
# => #<Arrow::Table:0x7fa3882cc300 ptr=0x7fa3ad260b00>
#   name	age
# 0	Tom 	 22
```

### Operations
Arrow compute functions can be accessed through `Arrow::Function`
```ruby
add = Arrow::Function.find('add')
add.execute([table['age'].data, table['age'].data]).value
# => #<Arrow::ChunkedArray:0x7fa389b87250 ptr=0x7fa3a4bb5c40 [
#   [
#     44,
#     46,
#     38
#   ]
# ]>
```

### Grouping
```ruby
table = Arrow::Table.new(
  'name' => ['Tom', 'Max', 'Kate', 'Tom'],
  'amount' => [10, 2, 3, 5]
)
table.group('name').sum('amount')
# => #<Arrow::Table:0x7fa389894ae8 ptr=0x7fa364141a50>
#   name	amount
# 0	Kate	     3
# 1	Max 	     2
# 2	Tom 	    15
```

### Joining
```ruby
amounts = Arrow::Table.new(
  'name' => ['Tom', 'Max', 'Kate'],
  'amount' => [10, 2, 3]
)
levels = Arrow::Table.new(
  'name' => ['Max', 'Kate', 'Tom'],
  'level' => [1, 9, 5]
)
amounts.join(levels, [:name])
# => #<Arrow::Table:0x55d512ceb1b0 ptr=0x55d51262aa70>
# 	name	amount	name	level
# 0	Tom 	    10	Tom 	    5
# 1	Max 	     2	Max 	    1
# 2	Kate	     3	Kate	    9
```
