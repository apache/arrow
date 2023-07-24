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

import fs from 'fs';
import path from 'path';
import child_process from 'child_process';
import stream from 'stream';
import util from 'util';
import asyncDoneSync from 'async-done';
const pump = stream.pipeline;
import { targets, modules } from './argv.js';
import { ReplaySubject, empty as ObservableEmpty, throwError as ObservableThrow, fromEvent as ObservableFromEvent } from 'rxjs';
import { share, flatMap, takeUntil, defaultIfEmpty, mergeWith } from 'rxjs/operators';
const asyncDone = util.promisify(asyncDoneSync);
import { createRequire } from 'module';
import esmRequire from './esm-require.cjs'

const require = createRequire(import.meta.url);

const mainExport = `Arrow`;
const npmPkgName = `apache-arrow`;
const npmOrgName = `@${npmPkgName}`;

const releasesRootDir = `targets`;
const knownTargets = [`es5`, `es2015`, `esnext`];
const knownModules = [`cjs`, `esm`, `cls`, `umd`];
const tasksToSkipPerTargetOrFormat = {
    src: { clean: true, build: true },
    cls: { test: true, package: true }
};
const packageJSONFields = [
    `version`, `license`, `description`,
    `author`, `homepage`, `repository`,
    `bugs`, `keywords`, `dependencies`,
    `bin`
];

const metadataFiles = [`LICENSE.txt`, `NOTICE.txt`, `README.md`].map((filename) => {
    const prefixes = [`./`, `../`];
    const p = prefixes.find((prefix) => {
        try {
            fs.statSync(path.resolve(path.join(prefix, filename)));
        } catch (e) { return false; }
        return true;
    });
    if (!p) {
        throw new Error(`Couldn't find ${filename} in ./ or ../`);
    }
    return path.join(p, filename);
});

// see: https://github.com/google/closure-compiler/blob/c1372b799d94582eaf4b507a4a22558ff26c403c/src/com/google/javascript/jscomp/CompilerOptions.java#L2988
const gCCLanguageNames = {
    es5: `ECMASCRIPT5`,
    es2015: `ECMASCRIPT_2015`,
    es2016: `ECMASCRIPT_2016`,
    es2017: `ECMASCRIPT_2017`,
    es2018: `ECMASCRIPT_2018`,
    es2019: `ECMASCRIPT_2019`,
    esnext: `ECMASCRIPT_NEXT`
};

function taskName(target, format) {
    return !format ? target : `${target}:${format}`;
}

function packageName(target, format) {
    return !format ? target : `${target}-${format}`;
}

function tsconfigName(target, format) {
    return !format ? target : `${target}.${format}`;
}

function targetDir(target, format) {
    return path.join(releasesRootDir, ...(!format ? [target] : [target, format]));
}

function shouldRunInChildProcess(target, format) {
    // If we're building more than one module/target, then yes run this task in a child process
    if (targets.length > 1 || modules.length > 1) { return true; }
    // If the target we're building *isn't* the target the gulp command was configured to run, then yes run that in a child process
    if (targets[0] !== target || modules[0] !== format) { return true; }
    // Otherwise no need -- either gulp was run for just one target, or we've been spawned as the child of a multi-target parent gulp
    return false;
}

const gulp = path.join(path.parse(require.resolve(`gulp`)).dir, `bin/gulp.js`);
function spawnGulpCommandInChildProcess(command, target, format) {
    const err = [];
    return asyncDone(() => {
        const child = child_process.spawn(
            `node`,
            [gulp, command, '-t', target, '-m', format, `-L`],
            {
                stdio: [`ignore`, `ignore`, `pipe`],
                env: { ...process.env, NODE_NO_WARNINGS: `1` }
            });
        child.stderr.on('data', (line) => err.push(line));
        return child;
    }).catch(() => Promise.reject(err.length > 0 ? err.join('\n')
        : `Error in "${command}:${taskName(target, format)}" task.`));
}

const logAndDie = (e) => { if (e) { console.error(e); process.exit(1); } };
function observableFromStreams(...streams) {
    if (streams.length <= 0) { return ObservableEmpty(); }
    const pumped = streams.length <= 1 ? streams[0] : pump(...streams, logAndDie);
    const fromEvent = ObservableFromEvent.bind(null, pumped);
    const streamObs = fromEvent(`data`).pipe(
        mergeWith(fromEvent(`error`).pipe(flatMap((e) => ObservableThrow(e)))),
        takeUntil(fromEvent(`end`).pipe(mergeWith(fromEvent(`close`)))),
        defaultIfEmpty(`empty stream`),
        share({ connector: () => new ReplaySubject(), resetOnError: false, resetOnComplete: false, resetOnRefCountZero: false })
    );
    streamObs.stream = pumped;
    streamObs.observable = streamObs;
    return streamObs;
}

function* combinations(_targets, _modules) {
    const targets = known(knownTargets, _targets || [`all`]);
    const modules = known(knownModules, _modules || [`all`]);

    if (_targets.includes(`src`)) {
        yield [`src`, ``];
        return;
    }

    if (_targets.includes(`all`) && _modules.includes(`all`)) {
        yield [`ts`, ``];
        yield [`src`, ``];
        yield [npmPkgName, ``];
    }

    for (const format of modules) {
        for (const target of targets) {
            yield [target, format];
        }
    }

    function known(known, values) {
        return values.includes(`all`) ? known
            : values.includes(`src`) ? [`src`]
                : Object.keys(
                    values.reduce((map, arg) => ((
                        (known.includes(arg)) &&
                        (map[arg.toLowerCase()] = true)
                        || true) && map
                    ), {})
                ).sort((a, b) => known.indexOf(a) - known.indexOf(b));
    }
}

const publicModulePaths = (dir) => [
    `${dir}/${mainExport}.dom.js`,
    `${dir}/util/int.js`,
];

export {
    mainExport, npmPkgName, npmOrgName, metadataFiles, packageJSONFields,

    knownTargets, knownModules, tasksToSkipPerTargetOrFormat, gCCLanguageNames,

    taskName, packageName, tsconfigName, targetDir, combinations, observableFromStreams,
    publicModulePaths, esmRequire, shouldRunInChildProcess, spawnGulpCommandInChildProcess,
};

export const targetAndModuleCombinations = [...combinations(targets, modules)];
