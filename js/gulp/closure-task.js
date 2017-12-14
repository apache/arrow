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
const ASTBuilders = require('ast-types').builders;
const transformAST = require('gulp-transform-js-ast');
const { Observable, ReplaySubject } = require('rxjs');
const closureCompiler = require('google-closure-compiler').gulp();

const closureTask = ((cache) => memoizeTask(cache, function closure(target, format) {
    const src = targetDir(target, `cls`);
    const out = targetDir(target, format);
    const entry = path.join(src, mainExport);
    const externs = path.join(src, `${mainExport}.externs`);
    return observableFromStreams(
        gulp.src([
/*   external libs first --> */ `node_modules/tslib/package.json`,
                                `node_modules/tslib/tslib.es6.js`,
                                `node_modules/flatbuffers/package.json`,
                                `node_modules/flatbuffers/js/flatbuffers.mjs`,
                                `node_modules/text-encoding-utf-8/package.json`,
                                `node_modules/text-encoding-utf-8/src/encoding.js`,
/*    then sources globs --> */ `${src}/**/*.js`,
/* and exclusions last -->  */ `!${src}/Arrow.externs.js`,
        ], { base: `./` }),
        sourcemaps.init(),
        closureCompiler(createClosureArgs(entry, externs)),
        // Strip out closure compiler's error-throwing iterator-return methods
        // see this issue: https://github.com/google/closure-compiler/issues/2728
        transformAST(iteratorReturnVisitor),
        // rename the sourcemaps from *.js.map files to *.min.js.map
        sourcemaps.write(`.`, { mapFile: (mapPath) => mapPath.replace(`.js.map`, `.${target}.min.js.map`) }),
        gulp.dest(out)
    ).publish(new ReplaySubject()).refCount();
}))({});

const createClosureArgs = (entry, externs) => ({
    third_party: true,
    warning_level: `QUIET`,
    dependency_mode: `STRICT`,
    rewrite_polyfills: false,
    externs: `${externs}.js`,
    entry_point: `${entry}.js`,
    module_resolution: `NODE`,
    // formatting: `PRETTY_PRINT`, debug: true,
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

const iteratorReturnVisitor = {
    visitObjectExpression(p) {
        const node = p.node, value = p.value;
        if (!node.properties || !(node.properties.length === 3)) { return value; }
        if (!propertyIsThrowingIteratorReturn(node.properties[2])) { return value; }
        value.properties = value.properties.slice(0, 2);
        return value;
    }
};

function propertyIsThrowingIteratorReturn(p) {
    if (!p || !(p.kind === 'init')) { return false; }
    if (!p.key || !(p.key.type === 'Identifier') || !(p.key.name === 'return')) { return false; }
    if (!p.value || !(p.value.type === 'FunctionExpression') || !p.value.params || !(p.value.params.length === 0)) { return false; }
    if (!p.value.body || !p.value.body.body || !(p.value.body.body.length === 1) || !(p.value.body.body[0].type === 'ThrowStatement')) { return false; }
    if (!p.value.body.body[0].argument || !(p.value.body.body[0].argument.type === 'CallExpression')) { return false; }
    if (!p.value.body.body[0].argument.arguments || !(p.value.body.body[0].argument.arguments.length === 1)) { return false; }
    if (!p.value.body.body[0].argument.arguments[0] || !(p.value.body.body[0].argument.arguments[0].type === 'Literal')) { return false; }
    return p.value.body.body[0].argument.arguments[0].value === 'Not yet implemented';
}