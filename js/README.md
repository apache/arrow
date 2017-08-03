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

### Installation

From this directory, run:

``` bash
$ npm install   # pull dependencies
$ npm run lint -- <filename>  # run tslint
$ npm run build # build typescript (run tsc and webpack)
$ npm run test  # run the unit tests (node.js only)
```

### Usage
The library is designed to be used with node.js or in the browser, this repository contains examples of both.

#### Node
Import the arrow module:

``` js
var arrow = require("arrow");
```

See [bin/arrow_schema.js](bin/arrow_schema.js) and [bin/arrow2csv.js](bin/arrow2csv.js) for usage examples.

#### Browser
Include `_bundles/arrow.js` in a `<script />` tag:
``` html
<script src="_bundles/arrow.js"/>
```
See [examples/read_file.html](examples/read_file.html) for a usage example.

### API
##### `arrow.getReader(buffer)`
Returns an `ArrowReader` object representing the Arrow file or stream contained in
the `buffer`.

##### `ArrowReader.loadNextBatch()`
Loads the next record batch and returns it's length.

##### `ArrowReader.getSchema()`
Returns a JSON representation of the file's Arrow schema.

##### `ArrowReader.getVectors()`
Returns a list of `Vector` objects, one for each column.
Vector objects have, at minimum, a `get(i)` method and a `length` attribute.

##### `ArrowReader.getVector(name: String)`
Return a Vector object for column `name`
