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

# The npm scripts

* `npm run clean` - cleans targets
* `npm run build` - cleans and compiles all targets
* `npm test` - executes tests against built targets

These npm scripts accept argument lists of targets × modules:

* Available `targets` are `es5`, `es2015`, `esnext`, and `all` (default: `all`)
* Available `modules` are `cjs`, `esm`, `umd`, and `all` (default: `all`)

Examples:

* `npm run build` -- builds all ES targets in all module formats
* `npm run build -- -t es5 -m all` -- builds the ES5 target in all module formats
* `npm run build -- -t all -m cjs` -- builds all ES targets in the CommonJS module format
* `npm run build -- --targets es5 es2015 -m all` -- builds the ES5 and ES2015 targets in all module formats
* `npm run build -- -t es5 --modules cjs esm` -- builds the ES5 target in CommonJS and ESModules module formats

This argument configuration also applies to `clean` and `test` scripts.

* `npm run deploy`

Uses [learna](https://github.com/lerna/lerna) to publish each build target to npm with [conventional](https://conventionalcommits.org/) [changelogs](https://github.com/conventional-changelog/conventional-changelog/tree/master/packages/conventional-changelog-cli).

# Updating the Arrow format flatbuffers generated code

Once generated, the flatbuffers format code needs to be adjusted for our TS and JS build environments.

## TypeScript

1. Generate the flatbuffers TypeScript source from the Arrow project root directory:
    ```sh
    cd $ARROW_HOME

    flatc --ts -o ./js/src/format ./format/*.fbs

    cd ./js/src/format

    # Delete Tensor_generated.js (skip this when we support Tensors)
    rm ./Tensor_generated.ts

    # Remove "_generated" suffix from TS files
    mv ./File_generated.ts .File.ts
    mv ./Schema_generated.ts .Schema.ts
    mv ./Message_generated.ts .Message.ts
    ```
1. Remove Tensor import from `Schema.ts`
1. Fix all the `flatbuffers` imports
    ```ts
    import { flatbuffers } from "./flatbuffers" // <-- change
    import { flatbuffers } from "flatbuffers" // <-- to this
    ```
1. Remove `_generated` from the ES6 imports of the generated files
    ```ts
    import * as NS16187549871986683199 from "./Schema_generated"; // <-- change
    import * as NS16187549871986683199 from "./Schema"; // <------- to this
    ```
1. Add `/* tslint:disable:class-name */` to the top of `Schema.ts`
1. Execute `npm run lint` to fix all the linting errors

## JavaScript (for Google Closure Compiler builds)

1. Generate the flatbuffers JS source from the Arrow project root directory
    ```sh
    cd $ARROW_HOME

    flatc --js --no-js-exports -o ./js/src/format ./format/*.fbs

    cd ./js/src/format

    # Delete Tensor_generated.js (skip this when we support Tensors)
    rm Tensor_generated.js

    # append an ES6 export to Schema_generated.js
    echo "$(cat Schema_generated.js)
    export { org };
    " > Schema_generated.js

    # import Schema's "org" namespace and
    # append an ES6 export to File_generated.js
    echo "import { org } from './Schema';
    $(cat File_generated.js)
    export { org };
    " > File_generated.js

    # import Schema's "org" namespace and
    # append an ES6 export to Message_generated.js
    echo "import { org } from './Schema';
    $(cat Message_generated.js)
    export { org };
    " > Message_generated.js
    ```
1. Fixup the generated JS enums with the reverse value-to-key mappings to match TypeScript
    `Message_generated.js`
    ```js
    // Replace this
    org.apache.arrow.flatbuf.MessageHeader = {
      NONE: 0,
      Schema: 1,
      DictionaryBatch: 2,
      RecordBatch: 3,
      Tensor: 4
    };
    // With this
    org.apache.arrow.flatbuf.MessageHeader = {
      NONE: 0, 0: 'NONE',
      Schema: 1, 1: 'Schema',
      DictionaryBatch: 2, 2: 'DictionaryBatch',
      RecordBatch: 3, 3: 'RecordBatch',
      Tensor: 4, 4: 'Tensor'
    };
    ```
    `Schema_generated.js`
    ```js
    /**
     * @enum
     */
    org.apache.arrow.flatbuf.MetadataVersion = {
      /**
       * 0.1.0
       */
      V1: 0, 0: 'V1',

      /**
       * 0.2.0
       */
      V2: 1, 1: 'V2',

      /**
       * 0.3.0 -> 0.7.1
       */
      V3: 2, 2: 'V3',

      /**
       * >= 0.8.0
       */
      V4: 3, 3: 'V4'
    };

    /**
     * @enum
     */
    org.apache.arrow.flatbuf.UnionMode = {
      Sparse: 0, 0: 'Sparse',
      Dense: 1, 1: 'Dense',
    };

    /**
     * @enum
     */
    org.apache.arrow.flatbuf.Precision = {
      HALF: 0, 0: 'HALF',
      SINGLE: 1, 1: 'SINGLE',
      DOUBLE: 2, 2: 'DOUBLE',
    };

    /**
     * @enum
     */
    org.apache.arrow.flatbuf.DateUnit = {
      DAY: 0, 0: 'DAY',
      MILLISECOND: 1, 1: 'MILLISECOND',
    };

    /**
     * @enum
     */
    org.apache.arrow.flatbuf.TimeUnit = {
      SECOND: 0, 0: 'SECOND',
      MILLISECOND: 1, 1: 'MILLISECOND',
      MICROSECOND: 2, 2: 'MICROSECOND',
      NANOSECOND: 3, 3: 'NANOSECOND',
    };

    /**
     * @enum
     */
    org.apache.arrow.flatbuf.IntervalUnit = {
      YEAR_MONTH: 0, 0: 'YEAR_MONTH',
      DAY_TIME: 1, 1: 'DAY_TIME',
    };

    /**
     * ----------------------------------------------------------------------
     * Top-level Type value, enabling extensible type-specific metadata. We can
     * add new logical types to Type without breaking backwards compatibility
     *
     * @enum
     */
    org.apache.arrow.flatbuf.Type = {
      NONE: 0, 0: 'NONE',
      Null: 1, 1: 'Null',
      Int: 2, 2: 'Int',
      FloatingPoint: 3, 3: 'FloatingPoint',
      Binary: 4, 4: 'Binary',
      Utf8: 5, 5: 'Utf8',
      Bool: 6, 6: 'Bool',
      Decimal: 7, 7: 'Decimal',
      Date: 8, 8: 'Date',
      Time: 9, 9: 'Time',
      Timestamp: 10, 10: 'Timestamp',
      Interval: 11, 11: 'Interval',
      List: 12, 12: 'List',
      Struct_: 13, 13: 'Struct_',
      Union: 14, 14: 'Union',
      FixedSizeBinary: 15, 15: 'FixedSizeBinary',
      FixedSizeList: 16, 16: 'FixedSizeList',
      Map: 17, 17: 'Map'
    };

    /**
     * ----------------------------------------------------------------------
     * The possible types of a vector
     *
     * @enum
     */
    org.apache.arrow.flatbuf.VectorType = {
      /**
       * used in List type, Dense Union and variable length primitive types (String, Binary)
       */
      OFFSET: 0, 0: 'OFFSET',

      /**
       * actual data, either wixed width primitive types in slots or variable width delimited by an OFFSET vector
       */
      DATA: 1, 1: 'DATA',

      /**
       * Bit vector indicating if each value is null
       */
      VALIDITY: 2, 2: 'VALIDITY',

      /**
       * Type vector used in Union type
       */
      TYPE: 3, 3: 'TYPE'
    };

    /**
     * ----------------------------------------------------------------------
     * Endianness of the platform producing the data
     *
     * @enum
     */
    org.apache.arrow.flatbuf.Endianness = {
      Little: 0, 0: 'Little',
      Big: 1, 1: 'Big',
    };
    ```
