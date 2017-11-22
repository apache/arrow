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

# [Apache Arrow](https://github.com/apache/arrow) in JS

[![Build Status](https://travis-ci.org/apache/arrow.svg?branch=master)](https://travis-ci.org/apache/arrow)
[![Coverage Status](https://coveralls.io/repos/github/apache/arrow/badge.svg)](https://coveralls.io/github/apache/arrow)

Arrow is a set of technologies that enable big-data systems to process and transfer data quickly.

## install [apache-arrow from npm](https://www.npmjs.com/package/apache-arrow)

`npm install apache-arrow`

(read about how we [package apache-arrow](#packaging) below)

# Powering Columnar In-Memory Analytics

Apache Arrow is a columnar memory layout specification for encoding vectors and table-like containers of flat and nested data. The Arrow spec aligns columnar data in memory to minimize cache misses and take advantage of the latest SIMD (Single input multiple data) and GPU operations on modern processors.

Apache Arrow is the emerging standard for large in-memory columnar data ([Spark](https://spark.apache.org/), [Pandas](http://wesmckinney.com/blog/pandas-and-apache-arrow/), [Drill](https://drill.apache.org/), ...). By standardizing on a common binary interchange format, big data systems can reduce the costs and friction associated with cross-system communication.

# Related Projects

* [GoAI](http://gpuopenanalytics.com/) -- Arrow-powered GPU analytics
* [rxjs-mapd](https://github.com/graphistry/rxjs-mapd) -- A MapD Core node-driver that returns query results as Arrow columns

# Usage

## Get a table from an Arrow file on disk

```es6
import { readFileSync } from 'fs';
import { Table } from 'apache-arrow';

const arrow = readFileSync('simple.arrow');
const table = Table.from([arrow]);

console.log(table.toString());

/*
 foo,  bar,  baz
   1,    1,   aa
null, null, null
   3, null, null
   4,    4,  bbb
   5,    5, cccc
*/
```

## Create a Table when the Arrow file is split across buffers

```es6
import { readFileSync } from 'fs';
import { Table } from 'apache-arrow';

const table = Table.from([
    'latlong/schema.arrow',
    'latlong/records.arrow'
].map((file) => readFileSync(file)));

console.log(table.toString());

/*
        origin_lat,         origin_lon
35.393089294433594,  -97.6007308959961
35.393089294433594,  -97.6007308959961
35.393089294433594,  -97.6007308959961
29.533695220947266, -98.46977996826172
29.533695220947266, -98.46977996826172
*/
```

## Columns are what you'd expect

```es6
import { readFileSync } from 'fs';
import { Table } from 'apache-arrow';

const table = Table.from([
    'latlong/schema.arrow',
    'latlong/records.arrow'
].map(readFileSync));

const column = table.col('origin_lat');
const typed = column.slice();

assert(typed instanceof Float32Array);

for (let i = -1, n = column.length; ++i < n;) {
    assert(column.get(i) === typed[i]);
}
```

## Usage with MapD Core

```es6
import MapD from 'rxjs-mapd';
import { Table } from 'apache-arrow';

const port = 9091;
const host = `localhost`;
const db = `mapd`;
const user = `mapd`;
const password = `HyperInteractive`;

MapD.open(host, port)
  .connect(db, user, password)
  .flatMap((session) =>
    // queryDF returns Arrow buffers
    session.queryDF(`
      SELECT origin_city
      FROM flights
      WHERE dest_city ILIKE 'dallas'
      LIMIT 5`
    ).disconnect()
  )
  .map(([schema, records]) =>
    // Create Arrow Table from results
    Table.from(schema, records))
  .map((table) =>
    // Stringify the table to CSV with row numbers
    table.toString({ index: true }))
  .subscribe((csvStr) =>
    console.log(csvStr));
/*
Index,   origin_city
    0, Oklahoma City
    1, Oklahoma City
    2, Oklahoma City
    3,   San Antonio
    4,   San Antonio
*/
```

# Getting involved

See [develop.md](https://github.com/apache/arrow/blob/master/develop.md)

Even if you do not plan to contribute to Apache Arrow itself or Arrow
integrations in other projects, we'd be happy to have you involved:

* Join the mailing list: send an email to
  [dev-subscribe@arrow.apache.org][1]. Share your ideas and use cases for the
  project.
* [Follow our activity on JIRA][3]
* [Learn the format][2]
* Contribute code to one of the reference implementations

We prefer to receive contributions in the form of GitHub pull requests. Please send pull requests against the [github.com/apache/arrow][4] repository.

If you are looking for some ideas on what to contribute, check out the [JIRA
issues][3] for the Apache Arrow project. Comment on the issue and/or contact
[dev@arrow.apache.org](http://mail-archives.apache.org/mod_mbox/arrow-dev/)
with your questions and ideas.

If you’d like to report a bug but don’t have time to fix it, you can still post
it on JIRA, or email the mailing list
[dev@arrow.apache.org](http://mail-archives.apache.org/mod_mbox/arrow-dev/)

## Packaging

`apache-arrow` is written in TypeScript, but the project is compiled to multiple JS versions and common module formats.

The base `apache-arrow` package includes all the compilation targets for convenience, but if you're conscientious about your `node_modules` footprint, we got you.

The targets are also published under the `@apache-arrow` namespace:

```sh
npm install apache-arrow # <-- combined es5/CommonJS + UMD, es2015/ESModules + UMD, and TypeScript package
npm install @apache-arrow/ts # standalone TypeScript package
npm install @apache-arrow/es5-cjs # standalone es5/CommonJS package
npm install @apache-arrow/es5-esm # standalone es5/ESModules package
npm install @apache-arrow/es5-umd # standalone es5/UMD package
npm install @apache-arrow/es2015-cjs # standalone es2015/CommonJS package
npm install @apache-arrow/es2015-esm # standalone es2015/ESModules package
npm install @apache-arrow/es2015-umd # standalone es2015/UMD package
npm install @apache-arrow/esnext-esm # standalone esNext/CommonJS package
npm install @apache-arrow/esnext-esm # standalone esNext/ESModules package
npm install @apache-arrow/esnext-umd # standalone esNext/UMD package
```

### Why we package like this

The JS community is a diverse group with a varied list of target environments and tool chains. Publishing multiple packages accommodates projects of all stripes.

If you think we missed a compilation target and it's a blocker for adoption, please open an issue.

# License

[Apache 2.0](https://github.com/apache/arrow/blob/master/LICENSE)

[1]: mailto:dev-subscribe@arrow.apache.org
[2]: https://github.com/apache/arrow/tree/master/format
[3]: https://issues.apache.org/jira/browse/ARROW
[4]: https://github.com/apache/arrow