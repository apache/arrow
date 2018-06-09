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
    mainExport, gCCLanguageNames,
    targetDir, observableFromStreams
} = require('./util');

const del = require('del');
const gulp = require('gulp');
const path = require('path');
const { promisify } = require('util');
const gulpRename = require(`gulp-rename`);
const { memoizeTask } = require('./memoize-task');
const exec = promisify(require('child_process').exec);
const { Observable, ReplaySubject } = require('rxjs');

const arrowTask = ((cache) => memoizeTask(cache, function copyMain(target, format) {
    const out = targetDir(target);
    const dtsGlob = `${targetDir(`es2015`, `cjs`)}/**/*.ts`;
    const cjsGlob = `${targetDir(`es2015`, `cjs`)}/**/*.js`;
    const esmGlob = `${targetDir(`es2015`, `esm`)}/**/*.js`;
    const es5UmdGlob = `${targetDir(`es5`, `umd`)}/*.js`;
    const es5UmdMaps = `${targetDir(`es5`, `umd`)}/*.map`;
    const es2015UmdGlob = `${targetDir(`es2015`, `umd`)}/*.js`;
    const es2015UmdMaps = `${targetDir(`es2015`, `umd`)}/*.map`;
    const ch_ext = (ext) => gulpRename((p) => { p.extname = ext; });
    const append = (ap) => gulpRename((p) => { p.basename += ap; });
    return Observable.forkJoin(
      observableFromStreams(gulp.src(dtsGlob), gulp.dest(out)), // copy d.ts files
      observableFromStreams(gulp.src(cjsGlob), gulp.dest(out)), // copy es2015 cjs files
      observableFromStreams(gulp.src(esmGlob), ch_ext(`.mjs`), gulp.dest(out)), // copy es2015 esm files and rename to `.mjs`
      observableFromStreams(gulp.src(es5UmdGlob), append(`.es5.min`), gulp.dest(out)), // copy es5 umd files and add `.min`
      observableFromStreams(gulp.src(es5UmdMaps),                     gulp.dest(out)), // copy es5 umd sourcemap files, but don't rename
      observableFromStreams(gulp.src(es2015UmdGlob), append(`.es2015.min`), gulp.dest(out)), // copy es2015 umd files and add `.es6.min`
      observableFromStreams(gulp.src(es2015UmdMaps),                        gulp.dest(out)), // copy es2015 umd sourcemap files, but don't rename
    ).publish(new ReplaySubject()).refCount();
}))({});

const arrowTSTask = ((cache) => memoizeTask(cache, async function copyTS(target, format) {
    const out = targetDir(target, format);
    await exec(`mkdirp ${out}`);
    await exec(`shx cp -r src/* ${out}`);
    await del(`${out}/**/*.js`);
}))({});
  
  
module.exports = arrowTask;
module.exports.arrowTask = arrowTask;
module.exports.arrowTSTask = arrowTSTask;