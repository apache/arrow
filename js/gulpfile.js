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

import { deleteAsync as del } from 'del';
import os from "os";
import gulp from "gulp";
import { targets } from "./gulp/argv.js";
import { from as ObservableFrom, bindNodeCallback as ObservableBindNodeCallback } from "rxjs";
import { mergeMap } from "rxjs/operators";
import cleanTask from "./gulp/clean-task.js";
import compileTask from "./gulp/compile-task.js";
import packageTask from "./gulp/package-task.js";
import { testTask, createTestData, cleanTestData } from "./gulp/test-task.js";
import { esbuildTask, rollupTask, webpackTask, execBundleTask } from "./gulp/bundle-task.js";
import { taskName, combinations, targetDir, knownTargets, npmPkgName, tasksToSkipPerTargetOrFormat, targetAndModuleCombinations } from "./gulp/util.js";

for (const [target, format] of combinations([`all`], [`all`])) {
    const task = taskName(target, format);
    gulp.task(`clean:${task}`, cleanTask(target, format));
    gulp.task(`test:${task}`, testTask(target, format));
    gulp.task(`compile:${task}`, compileTask(target, format));
    gulp.task(`package:${task}`, packageTask(target, format));
    gulp.task(`build:${task}`, gulp.series(
        `clean:${task}`, `compile:${task}`, `package:${task}`
    ));
}

// The UMD bundles build temporary es5/6/next targets via TS,
// then run the TS source through either closure-compiler or
// a minifier, so we special case that here.
knownTargets.forEach((target) => {
    const umd = taskName(target, `umd`);
    const cls = taskName(target, `cls`);
    gulp.task(`build:${umd}`, gulp.series(
        `build:${cls}`,
        `clean:${umd}`, `compile:${umd}`, `package:${umd}`,
        function remove_closure_tmp_files() {
            return del(targetDir(target, `cls`))
        }
    ));
});

// The main "apache-arrow" module builds the es2015/umd, es2015/cjs,
// es2015/esm, and esnext/umd targets, then copies and renames the
// compiled output into the apache-arrow folder
gulp.task(`build:${npmPkgName}`,
    gulp.series(
        gulp.parallel(
            `build:${taskName(`es2015`, `umd`)}`,
            `build:${taskName(`es2015`, `cjs`)}`,
            `build:${taskName(`es2015`, `esm`)}`,
            `build:${taskName(`esnext`, `umd`)}`
        ),
        `clean:${npmPkgName}`,
        `compile:${npmPkgName}`,
        `package:${npmPkgName}`
    )
);

// And finally the global composite tasks
gulp.task(`clean:testdata`, cleanTestData);
gulp.task(`create:testdata`, createTestData);
gulp.task(`test`, gulpConcurrent(getTasks(`test`)));
gulp.task(`clean`, gulp.parallel(getTasks(`clean`)));
gulp.task(`build`, gulpConcurrent(getTasks(`build`)));
gulp.task(`compile`, gulpConcurrent(getTasks(`compile`)));
gulp.task(`package`, gulpConcurrent(getTasks(`package`)));
gulp.task(`default`, gulp.series(`clean`, `build`, `test`));

gulp.task(`bundle:esbuild`, esbuildTask());
gulp.task(`bundle:rollup`, rollupTask());
gulp.task(`bundle:webpack`, webpackTask());
gulp.task(`bundle:webpack:analyze`, webpackTask({ analyze: true }));
gulp.task(`bundle:clean`, () => del(`test/bundle/**/*-bundle.js`));
gulp.task(`bundle:exec`, execBundleTask());

gulp.task(`bundle`, gulp.series(`bundle:clean`, `bundle:esbuild`, `bundle:rollup`, `bundle:webpack`, `bundle:exec`));

function gulpConcurrent(tasks, numCPUs = Math.max(1, os.cpus().length * 0.5) | 0) {
    return () => ObservableFrom(tasks.map((task) => gulp.series(task)))
        .pipe(mergeMap((task) => ObservableBindNodeCallback(task)(), numCPUs || 1));
}

function getTasks(name) {
    const tasks = [];
    if (targets.includes(`ts`)) tasks.push(`${name}:ts`);
    if (targets.includes(npmPkgName)) tasks.push(`${name}:${npmPkgName}`);
    for (const [target, format] of targetAndModuleCombinations) {
        if (tasksToSkipPerTargetOrFormat[target] && tasksToSkipPerTargetOrFormat[target][name]) continue;
        if (tasksToSkipPerTargetOrFormat[format] && tasksToSkipPerTargetOrFormat[format][name]) continue;
        tasks.push(`${name}:${taskName(target, format)}`);
    }
    return tasks.length && tasks || [(done) => done()];
}
