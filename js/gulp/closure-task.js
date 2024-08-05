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

import { targetDir, mainExport, gCCLanguageNames, publicModulePaths, observableFromStreams, shouldRunInChildProcess, spawnGulpCommandInChildProcess } from "./util.js";

import fs from 'node:fs';
import gulp from 'gulp';
import Path from 'node:path';
import https from 'node:https';
import { mkdirp } from 'mkdirp';
import { PassThrough } from 'node:stream';
import sourcemaps from 'gulp-sourcemaps';
import { memoizeTask } from './memoize-task.js';
import { compileBinFiles } from './typescript-task.js';

import closureCompiler from 'google-closure-compiler';
const compiler = closureCompiler.gulp();

import { fileURLToPath } from 'url';
import { dirname } from 'path';
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const closureCompilerVer = JSON.parse(fs.readFileSync(Path.join(__dirname, '..', 'package.json'))).devDependencies['google-closure-compiler'].split('.')[0];

export const closureTask = ((cache) => memoizeTask(cache, async function closure(target, format) {

    if (shouldRunInChildProcess(target, format)) {
        return spawnGulpCommandInChildProcess('compile', target, format);
    }

    const src = targetDir(target, `cls`);
    const srcAbsolute = Path.resolve(src);
    const out = targetDir(target, format);
    const externs = Path.join(`${out}/${mainExport}.externs.js`);
    const entry_point = Path.join(`${src}/${mainExport}.dom.cls.js`);

    const exportedImports = [];
    for (const publicModulePath of publicModulePaths(srcAbsolute)) {
        exportedImports.push({
            publicModulePath,
            exports_: getPublicExportedNames(await import(`file://${publicModulePath}`))
        });
    }

    await mkdirp(out);

    await Promise.all([
        fs.promises.writeFile(externs, generateExternsFile(exportedImports)),
        fs.promises.writeFile(entry_point, generateUMDExportAssignment(srcAbsolute, exportedImports))
    ]);

    const closeCompilerPolyfills = [];

    await Promise.all([
        runClosureCompileAsObservable().toPromise(),
        compileBinFiles(target, format).toPromise().then(() => Promise.all([
            observableFromStreams(gulp.src(`${src}/**/*.d.ts`), gulp.dest(out)).toPromise(), // copy .d.ts files,
            observableFromStreams(gulp.src(`${src}/**/*.d.ts.map`), gulp.dest(out)).toPromise(), // copy .d.ts.map files,
            observableFromStreams(gulp.src(`${src}/src/**/*`), gulp.dest(`${out}/src`)).toPromise(), // copy TS source files,
        ]))
    ]);

    // Download the closure compiler polyfill sources for sourcemaps
    await Promise.all(closeCompilerPolyfills.map(async (path) => {

        await fs.promises.mkdir(
            Path.join(out, Path.parse(path).dir),
            { recursive: true, mode: 0o755 }
        );

        const res = new PassThrough();
        const req = https.request(
            new URL(`https://raw.githubusercontent.com/google/closure-compiler/v${closureCompilerVer}/${path}`),
            (res_) => {
                if (res_.statusCode === 200) {
                    res_.pipe(res);
                } else {
                    res.end();
                }
            }
        );

        req.on('error', (e) => res.emit('error', e)).end();

        return observableFromStreams(res, fs.createWriteStream(Path.join(out, path))).toPromise();
    }));

    function runClosureCompileAsObservable() {
        return observableFromStreams(
            gulp.src([
                /* external libs first */
                `node_modules/flatbuffers/package.json`,
                `node_modules/flatbuffers/**/*.js`,
                `${src}/**/*.js` /* <-- then source globs */
            ], { base: `./` }),
            sourcemaps.init(),
            compiler(createClosureArgs(entry_point, externs, target), {
                platform: ['native', 'java', 'javascript']
            }),
            sourcemaps.mapSources((path) => {
                if (path.indexOf(`${src}/`) === 0) {
                    return path.slice(`${src}/`.length);
                }
                if (path.includes('com/google')) {
                    closeCompilerPolyfills.push(path);
                    return path.slice(`src/`.length);
                }
                return path;
            }),
            // rename the sourcemaps from *.js.map files to *.min.js.map
            sourcemaps.write(`./`, {
                sourceRoot: './src',
                includeContent: false,
                mapFile: (mapPath) => mapPath.replace(`.js.map`, `.${target}.min.js.map`),
            }),
            gulp.dest(out)
        );
    }
}))({});

export default closureTask;

const createClosureArgs = (entry_point, externs, target) => ({
    externs,
    entry_point,
    third_party: true,
    warning_level: `QUIET`,
    dependency_mode: `PRUNE`,
    rewrite_polyfills: false,
    module_resolution: `NODE`,
    // formatting: `PRETTY_PRINT`,
    // debug: true,
    compilation_level: `ADVANCED`,
    package_json_entry_names: `module,jsnext:main,main`,
    assume_function_wrapper: true,
    js_output_file: `${mainExport}.js`,
    language_in: gCCLanguageNames[`esnext`],
    language_out: gCCLanguageNames[target],
    output_wrapper: `${apacheHeader()}
(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? factory(exports) :
    typeof define === 'function' && define.amd ? define(['exports'], factory) :
    (factory(global.Arrow = global.Arrow || {}));
}(this, (function (exports) {%output%}.bind(this))));`
});

function generateUMDExportAssignment(src, exportedImports) {
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
/* eslint-disable */
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

var Encoding = function() {};
/** @type {?} */
Encoding[1] = function() {};
/** @type {?} */
Encoding[2] = function() {};
/** @type {?} */
Encoding.UTF8_BYTES = function() {};
/** @type {?} */
Encoding.UTF16_STRING = function() {};
`);
}

function getPublicExportedNames(entryModule) {
    const fn = function () { };
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
// under the License.`;
}
