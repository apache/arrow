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

import { metadataFiles, packageJSONFields, mainExport, npmPkgName, npmOrgName, targetDir, packageName, observableFromStreams } from './util.js';

import gulp from 'gulp';
import { memoizeTask } from './memoize-task.js';
import { ReplaySubject, EMPTY as ObservableEmpty, forkJoin as ObservableForkJoin } from 'rxjs';
import { share } from 'rxjs/operators';
import gulpJsonTransform from 'gulp-json-transform';

export const packageTask = ((cache) => memoizeTask(cache, function bundle(target, format) {
    if (target === `src`) return ObservableEmpty();
    const out = targetDir(target, format);
    const jsonTransform = gulpJsonTransform(target === npmPkgName ? createMainPackageJson(target, format) :
                                            target === `ts`       ? createTypeScriptPackageJson(target, format)
                                                                  : createScopedPackageJSON(target, format),
                                            2);
    return ObservableForkJoin([
        observableFromStreams(gulp.src(metadataFiles), gulp.dest(out)), // copy metadata files
        observableFromStreams(gulp.src(`package.json`), jsonTransform, gulp.dest(out)) // write packageJSONs
    ]).pipe(share({ connector: () => new ReplaySubject(), resetOnError: false, resetOnComplete: false, resetOnRefCountZero: false }));
}))({});

export default packageTask;

const createMainPackageJson = (target, format) => (orig) => ({
    ...createTypeScriptPackageJson(target, format)(orig),
    bin: orig.bin,
    name: npmPkgName,
    type: 'commonjs',
    main: `${mainExport}.node.js`,
    module: `${mainExport}.node.mjs`,
    browser: {
        [`./${mainExport}.node.js`]: `./${mainExport}.dom.js`,
        [`./${mainExport}.node.mjs`]: `./${mainExport}.dom.mjs`
    },
    exports: {
        '.': {
            node: {
                import: `./${mainExport}.node.mjs`,
                require: `./${mainExport}.node.js`,
            },
            import: `./${mainExport}.dom.mjs`,
            require: `./${mainExport}.dom.js`,
        },
        './*': {
            import: `./*.mjs`,
            require: `./*.js`
        }
    },
    types: `${mainExport}.node.d.ts`,
    unpkg: `${mainExport}.es2015.min.js`,
    jsdelivr: `${mainExport}.es2015.min.js`,
    sideEffects: false,
    esm: { mode: `all`, sourceMap: true }
});

const createTypeScriptPackageJson = (target, format) => (orig) => ({
    ...createScopedPackageJSON(target, format)(orig),
    bin: undefined,
    main: `${mainExport}.node.ts`,
    module: `${mainExport}.node.ts`,
    types: `${mainExport}.node.ts`,
    browser: `${mainExport}.dom.ts`,
    type: 'module',
    sideEffects: false,
    esm: { mode: `auto`, sourceMap: true },
    dependencies: {
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
            main:     format === 'umd' ? `${mainExport}.js` : `${mainExport}.node.js`,
            // set "type" to `module` or `commonjs` (https://nodejs.org/api/packages.html#packages_type)
            type:     format === 'esm' ? `module` : `commonjs`,
            // set "module" if building scoped ESM target
            module:   format === 'esm' ? `${mainExport}.node.js` : undefined,
            // set "sideEffects" to false as a hint to Webpack that it's safe to tree-shake the ESM target
            sideEffects: format === 'esm' ? false : undefined,
            // include "esm" settings for https://www.npmjs.com/package/esm if building scoped ESM target
            esm:      format === `esm` ? { mode: `auto`, sourceMap: true } : undefined,
            // set "types" (for TypeScript/VSCode)
            types:    format === 'umd' ? undefined : `${mainExport}.node.d.ts`,
        }
    )
);
