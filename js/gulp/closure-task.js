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
    esmRequire,
    gCCLanguageNames,
    publicModulePaths,
    observableFromStreams,
    shouldRunInChildProcess,
    spawnGulpCommandInChildProcess,
} = require('./util');

const fs = require('fs');
const gulp = require('gulp');
const path = require('path');
const sourcemaps = require('gulp-sourcemaps');
const { memoizeTask } = require('./memoize-task');
const { compileBinFiles } = require('./typescript-task');
const mkdirp = require('util').promisify(require('mkdirp'));
const closureCompiler = require('google-closure-compiler').gulp();

const closureTask = ((cache) => memoizeTask(cache, async function closure(target, format) {

    if (shouldRunInChildProcess(target, format)) {
        return spawnGulpCommandInChildProcess('compile', target, format);
    }

    const src = targetDir(target, `cls`);
    const srcAbsolute = path.resolve(src);
    const out = targetDir(target, format);
    const externs = path.join(`${out}/${mainExport}.externs.js`);
    const entry_point = path.join(`${src}/${mainExport}.dom.cls.js`);

    const exportedImports = publicModulePaths(srcAbsolute).reduce((entries, publicModulePath) => [
        ...entries, {
            publicModulePath,
            exports_: getPublicExportedNames(esmRequire(publicModulePath, { warnings: false }))
        }
    ], []);

    await mkdirp(out);

    await Promise.all([
        fs.promises.writeFile(externs, generateExternsFile(exportedImports)),
        fs.promises.writeFile(entry_point, generateUMDExportAssignnent(srcAbsolute, exportedImports))
    ]);

    return await Promise.all([
        runClosureCompileAsObservable().toPromise(),
        compileBinFiles(target, format).toPromise()
    ]);

    function runClosureCompileAsObservable() {
        return observableFromStreams(
            gulp.src([
                /* external libs first */
                `node_modules/flatbuffers/package.json`,
                `node_modules/flatbuffers/js/flatbuffers.mjs`,
                `node_modules/text-encoding-utf-8/package.json`,
                `node_modules/text-encoding-utf-8/src/encoding.js`,
                `${src}/**/*.js` /* <-- then source globs */
            ], { base: `./` }),
            sourcemaps.init(),
            closureCompiler(createClosureArgs(entry_point, externs)),
            // rename the sourcemaps from *.js.map files to *.min.js.map
            sourcemaps.write(`.`, { mapFile: (mapPath) => mapPath.replace(`.js.map`, `.${target}.min.js.map`) }),
            gulp.dest(out)
        );
    }
}))({});

module.exports = closureTask;
module.exports.closureTask = closureTask;

const createClosureArgs = (entry_point, externs) => ({
    externs,
    entry_point,
    third_party: true,
    warning_level: `QUIET`,
    dependency_mode: `STRICT`,
    rewrite_polyfills: false,
    module_resolution: `NODE`,
    // formatting: `PRETTY_PRINT`,
    // debug: true,
    compilation_level: `ADVANCED`,
    allow_method_call_decomposing: true,
    package_json_entry_names: `module,jsnext:main,main`,
    assume_function_wrapper: true,
    js_output_file: `${mainExport}.js`,
    language_in: gCCLanguageNames[`esnext`],
    language_out: gCCLanguageNames[`es5`],
    output_wrapper:`${apacheHeader()}
(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
    typeof define === 'function' && define.amd ? define(['exports'], factory) :
    (factory(global.Arrow = global.Arrow || {}));
}(this, (function (exports) {%output%}.bind(this))));`
});

function generateUMDExportAssignnent(src, exportedImports) {
    return [
        ...exportedImports.map(({ publicModulePath }, i) => {
            const p = publicModulePath.slice(src.length + 1);
            return (`import * as exports${i} from './${p}';`);
        }).filter(Boolean),
        'Object.assign(arguments[0], exports0);'
    ].join('\n');
}

function generateExternsFile(exportedImports) {
    return [
        externsHeader(),
        ...exportedImports.reduce((externBodies, { exports_ }) => [
            ...externBodies, ...exports_.map(externBody)
        ], []).filter(Boolean)
    ].join('\n');
}

function externBody({ exportName, staticNames, instanceNames }) {
    return [
        `var ${exportName} = function() {};`,
        staticNames.map((staticName) => (isNaN(+staticName)
            ? `/** @type {?} */\n${exportName}.${staticName} = function() {};`
            : `/** @type {?} */\n${exportName}[${staticName}] = function() {};`
        )).join('\n'),
        instanceNames.map((instanceName) => (isNaN(+instanceName)
            ? `/** @type {?} */\n${exportName}.prototype.${instanceName};`
            : `/** @type {?} */\n${exportName}.prototype[${instanceName}];`
        )).join('\n')
    ].filter(Boolean).join('\n');
}

function externsHeader() {
    return (`${apacheHeader()}
// @ts-nocheck
/* tslint:disable */
/**
 * @fileoverview Closure Compiler externs for Arrow
 * @externs
 * @suppress {duplicate,checkTypes}
 */
/** @type {symbol} */
Symbol.iterator;
/** @type {symbol} */
Symbol.toPrimitive;
/** @type {symbol} */
Symbol.asyncIterator;
`);
}

function getPublicExportedNames(entryModule) {
    const fn = function() {};
    const isStaticOrProtoName = (x) => (
        !(x in fn) &&
        (x !== `default`) &&
        (x !== `undefined`) &&
        (x !== `__esModule`) &&
        (x !== `constructor`) &&
        !(x.startsWith('_'))
    );
    return Object
        .getOwnPropertyNames(entryModule)
        .filter((name) => name !== 'default')
        .filter((name) => (
            typeof entryModule[name] === `object` ||
            typeof entryModule[name] === `function`
        ))
        .map((name) => [name, entryModule[name]])
        .reduce((reserved, [name, value]) => {

            const staticNames = value &&
                typeof value === 'object' ? Object.getOwnPropertyNames(value).filter(isStaticOrProtoName) :
                typeof value === 'function' ? Object.getOwnPropertyNames(value).filter(isStaticOrProtoName) : [];

            const instanceNames = (typeof value === `function` && Object.getOwnPropertyNames(value.prototype || {}) || []).filter(isStaticOrProtoName);

            return [...reserved, { exportName: name, staticNames, instanceNames }];
        }, []);
}

function apacheHeader() {
    return `// Licensed to the Apache Software Foundation (ASF) under one
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
// under the License.`
}
