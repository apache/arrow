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

import { targetDir, tsconfigName, observableFromStreams, shouldRunInChildProcess, spawnGulpCommandInChildProcess } from './util.js';

import gulp from 'gulp';
import path from 'path';
import tsc from 'typescript';
import ts from 'gulp-typescript';
import * as fs from 'fs/promises';
import sourcemaps from 'gulp-sourcemaps';
import { memoizeTask } from './memoize-task.js';
import { ReplaySubject, forkJoin as ObservableForkJoin, defer as ObservableDefer } from 'rxjs';
import { mergeWith, takeLast, share, concat } from 'rxjs/operators';

export const typescriptTask = ((cache) => memoizeTask(cache, function typescript(target, format) {
    if (shouldRunInChildProcess(target, format)) {
        return spawnGulpCommandInChildProcess('compile', target, format);
    }

    const out = targetDir(target, format);
    const tsconfigPath = path.join(`tsconfig`, `tsconfig.${tsconfigName(target, format)}.json`);
    return compileTypescript(out, tsconfigPath)
        .pipe(mergeWith(compileBinFiles(target, format)))
        .pipe(takeLast(1))
        .pipe(share({ connector: () => new ReplaySubject(), resetOnError: false, resetOnComplete: false, resetOnRefCountZero: false }))
}))({});

export default typescriptTask;

export function compileBinFiles(target, format) {
    const out = targetDir(target, format);
    const tsconfigPath = path.join(`tsconfig`, `tsconfig.${tsconfigName('bin', 'cjs')}.json`);
    const tsconfigOverrides = format === 'esm' ? { target, module: 'ES2015' } : { target };
    return compileTypescript(out, tsconfigPath, tsconfigOverrides, false)
      .pipe(takeLast(1))
      .pipe(concat(ObservableDefer(() => {
        return fs.chmod(path.join(out, 'bin', 'arrow2csv.js'), 0o755);
      })));
}

function compileTypescript(out, tsconfigPath, tsconfigOverrides, writeSourcemaps = true) {
    const tsProject = ts.createProject(tsconfigPath, { typescript: tsc, ...tsconfigOverrides });
    const { stream: { js, dts } } = observableFromStreams(
        tsProject.src(), sourcemaps.init(),
        tsProject(ts.reporter.defaultReporter())
    );
    const writeSources = observableFromStreams(tsProject.src(), gulp.dest(path.join(out, 'src')));
    const writeDTypes = observableFromStreams(dts, sourcemaps.write('./', { includeContent: false, sourceRoot: './src' }), gulp.dest(out));
    const mapFile = tsProject.options.module === tsc.ModuleKind.ES2015 ? esmMapFile : cjsMapFile;
    const writeJSArgs = writeSourcemaps ? [
        js,
        sourcemaps.write('./', { mapFile, includeContent: false, sourceRoot: './src' }),
        gulp.dest(out)
      ] : [
        js,
        gulp.dest(out)
      ];
    const writeJS = observableFromStreams(...writeJSArgs);
    return ObservableForkJoin([writeSources, writeDTypes, writeJS]);
}

const cjsMapFile = (mapFilePath) => mapFilePath;
const esmMapFile = (mapFilePath) => mapFilePath.replace('.js.map', '.mjs.map');
