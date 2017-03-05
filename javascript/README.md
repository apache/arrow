### Installation

From this directory, run:

``` bash
$ npm install   # pull dependencies
$ tsc           # build typescript
$ webpack       # bundle for the browser
```

### Usage
The library is designed to be used with node.js or in the browser, this repository contains examples of both.

#### Node
Import the arrow module:

``` js
var arrow = require("arrow.js");
```

See [bin/arrow_schema.js](bin/arrow_schema.js) and [bin/arrow2csv.js](bin/arrow2csv.js) for usage examples.

#### Browser
Include `dist/arrow-bundle.js` in a `<script />` tag:
``` html
<script src="arrow-bundle.js"/>
```
See [examples/read_file.html](examples/read_file.html) for a usage example - or try it out now at [theneuralbit.github.io/arrow](http://theneuralbit.github.io/arrow)

### API
##### `arrow.loadSchema(buffer)`
Returns a JSON representation of the file's Arrow schema.

##### `arrow.loadVectors(buffer)`
Returns a dictionary of `Vector` objects, one for each column, indexed by the column's name. 
Vector objects have, at minimum, a `get(i)` method and a `length` attribute. 
