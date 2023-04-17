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

* Join the mailing list: send an email to [dev-subscribe@arrow.apache.org][1].
  Share your ideas and use cases for the project
* Follow our activity on [GitHub issues][3]
* [Learn the format][2]
* Contribute code to one of the reference implementations

We prefer to receive contributions in the form of GitHub pull requests.
Please send pull requests against the [github.com/apache/arrow][4] repository.

If you are looking for some ideas on what to contribute, check out the [GitHub
issues][3] for the Apache Arrow project. Comment on the issue and/or contact
[dev@arrow.apache.org](http://mail-archives.apache.org/mod_mbox/arrow-dev/)
with your questions and ideas.

If you’d like to report a bug but don’t have time to fix it, you can still post
it on GitHub issues, or email the mailing list
[dev@arrow.apache.org](http://mail-archives.apache.org/mod_mbox/arrow-dev/)

# The package.json scripts

We use [yarn](https://yarnpkg.com/) to install dependencies and run scrips.

* `yarn clean` - cleans targets
* `yarn build` - cleans and compiles all targets
* `yarn test` - executes tests against built targets

These scripts accept argument lists of targets × modules:

* Available `targets` are `es5`, `es2015`, `esnext`, `ts`, and `all` (default: `all`)
* Available `modules` are `cjs`, `esm`, `umd`, and `all` (default: `all`)

Examples:

* `yarn build` -- builds all ES targets in all module formats
* `yarn build -t es5 -m all` -- builds the ES5 target in all module formats
* `yarn build -t all -m cjs` -- builds all ES targets in the CommonJS module format
* `yarn build -t es5 -t es2015 -m all` -- builds the ES5 and ES2015 targets in all module formats
* `yarn build -t es5 -m cjs -m esm` -- builds the ES5 target in CommonJS and ESModules module formats

This argument configuration also applies to `clean` and `test` scripts.

To run tests on the bundles, you need to build them first.
To run tests directly on the sources without bundling, use the `src` target (e.g. `yarn test -t src`).

* `yarn deploy`

Uses [lerna](https://github.com/lerna/lerna) to publish each build target to npm with [conventional](https://conventionalcommits.org/) [changelogs](https://github.com/conventional-changelog/conventional-changelog/tree/master/packages/conventional-changelog-cli).

* `yarn doc`

Compiles the documentation with [Typedoc](https://typedoc.org/). Use `yarn doc --watch` to automatically rebuild when the docs change.

# Running the Performance Benchmarks

You can run the benchmarks with `yarn perf`. To print the results to stderr as JSON, add the `--json` flag (e.g. `yarn perf --json 2> perf.json`).

You can change the target you want to test by changing the imports in `perf/index.ts`. Note that you need to compile the bundles with `yarn build` before you can import them.

# Testing Bundling

The bunldes use `apache-arrow` so make sure to build it with `yarn build -t apache-arrow`. To bundle with a variety of bundlers, run `yarn test:bundle` or `yarn gulp bundle`.

Run `yarn gulp bundle:webpack:analyze` to open [Webpack Bundle Analyzer](https://github.com/webpack-contrib/webpack-bundle-analyzer).

# Updating the Arrow format flatbuffers generated code

1. Once generated, the flatbuffers format code needs to be adjusted for our build scripts (assumes `gnu-sed`):

    ```shell
    cd $ARROW_HOME

    # Create a tmpdir to store modified flatbuffers schemas
    tmp_format_dir=$(mktemp -d)
    cp ./format/*.fbs $tmp_format_dir

    # Remove namespaces from the flatbuffers schemas
    sed -i '+s+namespace org.apache.arrow.flatbuf;++ig' $tmp_format_dir/*.fbs
    sed -i '+s+org.apache.arrow.flatbuf.++ig' $tmp_format_dir/*.fbs

    # Generate TS source from the modified Arrow flatbuffers schemas
    flatc --ts -o ./js/src/fb $tmp_format_dir/{File,Schema,Message,Tensor,SparseTensor}.fbs

    # Remove the tmpdir
    rm -rf $tmp_format_dir
    ```

2. Manually fix the unused imports and add // @ts-ignore for other errors

3. Add `.js` to the imports. In VSCode, you can search for `^(import [^';]* from '(\./|(\.\./)+)[^';.]*)';` and replace with `$1.js';`.

4. Execute `yarn lint` from the `js` directory to fix the linting errors

[1]: mailto:dev-subscribe@arrow.apache.org
[2]: https://github.com/apache/arrow/tree/main/format
[3]: https://github.com/apache/arrow/issues
[4]: https://github.com/apache/arrow
