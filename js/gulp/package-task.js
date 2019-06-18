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
    metadataFiles, packageJSONFields,
    mainExport, npmPkgName, npmOrgName,
    targetDir, packageName, observableFromStreams
} = require('./util');

const gulp = require('gulp');
const { memoizeTask } = require('./memoize-task');
const { Observable, ReplaySubject } = require('rxjs');
const gulpJsonTransform = require('gulp-json-transform');

const packageTask = ((cache) => memoizeTask(cache, function bundle(target, format) {
    if (target === `src`) return Observable.empty();
    const out = targetDir(target, format);
    const jsonTransform = gulpJsonTransform(target === npmPkgName ? createMainPackageJson(target, format) :
                                            target === `ts`       ? createTypeScriptPackageJson(target, format)
                                                                  : createScopedPackageJSON(target, format),
                                            2);
    return Observable.forkJoin(
      observableFromStreams(gulp.src(metadataFiles), gulp.dest(out)), // copy metadata files
      observableFromStreams(gulp.src(`package.json`), jsonTransform, gulp.dest(out)) // write packageJSONs
    ).publish(new ReplaySubject()).refCount();
}))({});

module.exports = packageTask;
module.exports.packageTask = packageTask;

const createMainPackageJson = (target, format) => (orig) => ({
    ...createTypeScriptPackageJson(target, format)(orig),
    bin: orig.bin,
    name: npmPkgName,
    main: `${mainExport}.node`,
    browser: `${mainExport}.dom`,
    module: `${mainExport}.dom.mjs`,
    types: `${mainExport}.node.d.ts`,
    unpkg: `${mainExport}.es5.min.js`,
    jsdelivr: `${mainExport}.es5.min.js`,
    esm: { mode: `all`, sourceMap: true }
});
  
const createTypeScriptPackageJson = (target, format) => (orig) => ({
    ...createScopedPackageJSON(target, format)(orig),
    bin: undefined,
    module: undefined,
    main: `${mainExport}.node.ts`,
    types: `${mainExport}.node.ts`,
    browser: `${mainExport}.dom.ts`,
    dependencies: {
        '@types/flatbuffers': '*',
        '@types/node': '*',
        ...orig.dependencies
    }
});
  
const createScopedPackageJSON = (target, format) => (({ name, ...orig }) =>
    packageJSONFields.reduce(
        (xs, key) => ({ ...xs, [key]: xs[key] || orig[key] }),
        {
            // un-set version, since it's automatically applied during the release process
            version: undefined,
            // set the scoped package name (e.g. "@apache-arrow/esnext-esm")
            name: `${npmOrgName}/${packageName(target, format)}`,
            // set "unpkg"/"jsdeliver" if building scoped UMD target
            unpkg:    format === 'umd' ? `${mainExport}.js` : undefined,
            jsdelivr: format === 'umd' ? `${mainExport}.js` : undefined,
            // set "browser" if building scoped UMD target, otherwise "Arrow.dom"
            browser:  format === 'umd' ? `${mainExport}.js` : `${mainExport}.dom.js`,
            // set "main" to "Arrow" if building scoped UMD target, otherwise "Arrow.node"
            main:     format === 'umd' ? `${mainExport}.js` : `${mainExport}.node`,
            // set "module" (for https://www.npmjs.com/package/@pika/pack) if building scoped ESM target
            module:   format === 'esm' ? `${mainExport}.dom.js` : undefined,
            // include "esm" settings for https://www.npmjs.com/package/esm if building scoped ESM target
            esm:      format === `esm` ? { mode: `auto`, sourceMap: true } : undefined,
            // set "types" (for TypeScript/VSCode)
            types:    format === 'umd' ? undefined : `${mainExport}.node.d.ts`,
        }
    )
);
