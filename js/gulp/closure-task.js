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
const { Observable, ReplaySubject } = require('rxjs');
const closureCompiler = require('google-closure-compiler').gulp();

const closureTask = ((cache) => memoizeTask(cache, function closure(target, format) {
    const src = targetDir(target, `cls`);
    const out = targetDir(target, format);
    const entry = path.join(src, mainExport);
    const externs = path.join(src, `${mainExport}.externs`);
    return observableFromStreams(
        gulp.src([
/*   external libs first --> */ `closure-compiler-scripts/*.js`,
/*    then sources glob --> */ `${src}/**/*.js`,
/* and exclusions last --> */ `!${src}/format/*.js`,
                              `!${src}/Arrow.externs.js`,
        ], { base: `./` }),
        sourcemaps.init(),
        closureCompiler(createClosureArgs(entry, externs)),
        // rename the sourcemaps from *.js.map files to *.min.js.map
        sourcemaps.write(`.`, { mapFile: (mapPath) => mapPath.replace(`.js.map`, `.${target}.min.js.map`) }),
        gulp.dest(out)
    ).publish(new ReplaySubject()).refCount();
}))({});

const createClosureArgs = (entry, externs) => ({
    third_party: true,
    warning_level: `QUIET`,
    dependency_mode: `LOOSE`,
    rewrite_polyfills: false,
    externs: `${externs}.js`,
    entry_point: `${entry}.js`,
    // formatting: `PRETTY_PRINT`,
    compilation_level: `ADVANCED`,
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
