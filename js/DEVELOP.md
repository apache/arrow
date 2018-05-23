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

# Getting Involved
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

Uses [lerna](https://github.com/lerna/lerna) to publish each build target to npm with [conventional](https://conventionalcommits.org/) [changelogs](https://github.com/conventional-changelog/conventional-changelog/tree/master/packages/conventional-changelog-cli).

# Updating the Arrow format flatbuffers generated code

Once generated, the flatbuffers format code needs to be adjusted for our build scripts.

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
