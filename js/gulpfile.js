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

const del = require('del');
const gulp = require('gulp');
const path = require('path');
const { Observable } = require('rxjs');
const buildTask = require('./gulp/build-task');
const cleanTask = require('./gulp/clean-task');
const packageTask = require('./gulp/package-task');
const { targets, modules } = require('./gulp/argv');
const { testTask, createTestData, cleanTestData } = require('./gulp/test-task');
const {
    targetDir,
    taskName, combinations,
    knownTargets, knownModules,
    npmPkgName, UMDSourceTargets,
    moduleFormatsToSkipCombosOf
} = require('./gulp/util');

for (const [target, format] of combinations([`all`], [`all`])) {
    const task = taskName(target, format);
    gulp.task(`clean:${task}`, cleanTask(target, format));
    gulp.task( `test:${task}`,  testTask(target, format));
    gulp.task(`debug:${task}`,  testTask(target, format, true));
    gulp.task(`build:${task}`, gulp.series(`clean:${task}`,
                                            buildTask(target, format),
                                            packageTask(target, format)));
}

// The UMD bundles build temporary es5/6/next targets via TS,
// then run the TS source through either closure-compiler or
// uglify, so we special case that here.
knownTargets.forEach((target) =>
    gulp.task(`build:${target}:umd`,
        gulp.series(
            gulp.parallel(
                cleanTask(target, `umd`),
                cleanTask(UMDSourceTargets[target], `cls`)
            ),
            buildTask(UMDSourceTargets[target], `cls`),
            buildTask(target, `umd`), packageTask(target, `umd`)
        )
    )
);

// The main "apache-arrow" module builds the es5/cjs, es5/umd,
// es2015/esm, es2015/umd, and ts targets, then copies and
// renames the compiled output into the apache-arrow folder
gulp.task(`build:${npmPkgName}`,
    gulp.series(
        cleanTask(npmPkgName),
        gulp.parallel(
            `build:${taskName(`es5`, `cjs`)}`,
            `build:${taskName(`es5`, `umd`)}`,
            `build:${taskName(`es2015`, `esm`)}`,
            `build:${taskName(`es2015`, `umd`)}`
        ),
        buildTask(npmPkgName), packageTask(npmPkgName)
    )
);


function gulpConcurrent(tasks) {
    return () => Observable.bindCallback((tasks, cb) => gulp.parallel(tasks)(cb))(tasks);
}
  
const buildConcurrent = (tasks) => () =>
    gulpConcurrent(tasks)()
        .concat(Observable
            .defer(() => Observable
            .merge(...knownTargets.map((target) =>
                del(`${targetDir(target, `cls`)}/**`)))));
  
gulp.task(`clean:testdata`, cleanTestData);
gulp.task(`create:testdata`, createTestData);
gulp.task( `test`, gulp.series(getTasks(`test`)));
gulp.task(`debug`, gulp.series(getTasks(`debug`)));
gulp.task(`clean`, gulp.parallel(getTasks(`clean`)));
gulp.task(`build`, buildConcurrent(getTasks(`build`)));
gulp.task(`default`,  gulp.series(`build`, `test`));
  
function getTasks(name) {
    const tasks = [];
    if (targets.indexOf(`ts`) !== -1) tasks.push(`${name}:ts`);
    if (targets.indexOf(npmPkgName) !== -1) tasks.push(`${name}:${npmPkgName}`);
    for (const [target, format] of combinations(targets, modules)) {
        if (moduleFormatsToSkipCombosOf[format] && name === `test`) {
            continue;
        }
        tasks.push(`${name}:${taskName(target, format)}`);
    }
    return tasks.length && tasks || [(done) => done()];
}
