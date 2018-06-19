// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

const {
    targetDir,
    mainExport,
    gCCLanguageNames,
    UMDSourceTargets,
    observableFromStreams
} = require('./util');

const gulp = require('gulp');
const path = require('path');
const sourcemaps = require('gulp-sourcemaps');
const { memoizeTask } = require('./memoize-task');
const { compileBinFiles } = require('./typescript-task');
const { Observable, ReplaySubject } = require('rxjs');
const closureCompiler = require('google-closure-compiler').gulp();

const closureTask = ((cache) => memoizeTask(cache, function closure(target, format) {
    const src = targetDir(target, `cls`);
    const out = targetDir(target, format);
    const entry = path.join(src, mainExport);
    const externs = path.join(`src/Arrow.externs.js`);
    return observableFromStreams(
        gulp.src([
/*   external libs first --> */ `node_modules/tslib/package.json`,
                                `node_modules/tslib/tslib.es6.js`,
                                `node_modules/flatbuffers/package.json`,
                                `node_modules/flatbuffers/js/flatbuffers.mjs`,
                                `node_modules/text-encoding-utf-8/package.json`,
                                `node_modules/text-encoding-utf-8/src/encoding.js`,
/*    then sources globs --> */ `${src}/**/*.js`,
        ], { base: `./` }),
        sourcemaps.init(),
        closureCompiler(createClosureArgs(entry, externs)),
        // rename the sourcemaps from *.js.map files to *.min.js.map
        sourcemaps.write(`.`, { mapFile: (mapPath) => mapPath.replace(`.js.map`, `.${target}.min.js.map`) }),
        gulp.dest(out)
    )
    .merge(compileBinFiles(target, format))
    .takeLast(1)
    .publish(new ReplaySubject()).refCount();
}))({});

const createClosureArgs = (entry, externs) => ({
    externs,
    third_party: true,
    warning_level: `QUIET`,
    dependency_mode: `STRICT`,
    rewrite_polyfills: false,
    entry_point: `${entry}.js`,
    module_resolution: `NODE`,
    // formatting: `PRETTY_PRINT`,
    // debug: true,
    compilation_level: `ADVANCED`,
    allow_method_call_decomposing: true,
    package_json_entry_names: `module,jsnext:main,main`,
    assume_function_wrapper: true,
    js_output_file: `${mainExport}.js`,
    language_in: gCCLanguageNames[`es2015`],
    language_out: gCCLanguageNames[`es5`],
    output_wrapper:
`// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
    typeof define === 'function' && define.amd ? define(['exports'], factory) :
    (factory(global.Arrow = global.Arrow || {}));
}(this, (function (exports) {%output%}.bind(this))));`
});

module.exports = closureTask;
module.exports.closureTask = closureTask;
