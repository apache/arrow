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

const fs = require('fs');
const path = require(`path`);
const pump = require(`pump`);
const { Observable, ReplaySubject } = require('rxjs');

const mainExport = `Arrow`;
const npmPkgName = `apache-arrow`;
const npmOrgName = `@${npmPkgName}`;

const releasesRootDir = `targets`;
const knownTargets = [`es5`, `es2015`, `esnext`];
const knownModules = [`cjs`, `esm`, `cls`, `umd`];
const moduleFormatsToSkipCombosOf = { cls: true };
const packageJSONFields = [
  `version`, `license`, `description`,
  `author`, `homepage`, `repository`,
  `bugs`, `keywords`,  `dependencies`
];

const metadataFiles = [`LICENSE.txt`, `NOTICE.txt`, `README.md`].map((filename) => {
    let err = false, prefixes = [`./`, `../`];
    let p = prefixes.find((prefix) => {
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
 esnext: `ECMASCRIPT_NEXT`
};

const UMDSourceTargets = {
    es5: `es5`,
 es2015: `es2015`,
 es2016: `es2015`,
 es2017: `es2015`,
 esnext: `es2015`
};

const uglifyLanguageNames = {
    es5: 5, es2015: 6,
 es2016: 7, es2017: 8,
 esnext: 8 // <--- ?
};

// ES7+ keywords Uglify shouldn't mangle
// Hardcoded here since some are from ES7+, others are
// only defined in interfaces, so difficult to get by reflection.
const ESKeywords = [
    // PropertyDescriptors
    `configurable`, `enumerable`,
    // IteratorResult, Symbol.asyncIterator
    `done`, `value`, `Symbol.asyncIterator`, `asyncIterator`,
    // AsyncObserver
    `values`, `hasError`, `hasCompleted`,`errorValue`, `closed`,
    // Observable/Subscription/Scheduler
    `next`, `error`, `complete`, `subscribe`, `unsubscribe`, `isUnsubscribed`,
    // EventTarget
    `addListener`, `removeListener`, `addEventListener`, `removeEventListener`,
    // Arrow properties
    `low`, `high`, `data`, `index`, `field`, `validity`, `columns`, `fieldNode`, `subarray`,
];

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

function logAndDie(e) {
    if (e) {
        console.error(e);
        process.exit(1);
    }
}

function observableFromStreams(...streams) {
    const pumped = streams.length <= 1 ? streams[0]
        : pump(...streams, logAndDie);
    const fromEvent = Observable.fromEvent.bind(null, pumped);
    const streamObs = fromEvent(`data`)
               .merge(fromEvent(`error`).flatMap((e) => Observable.throw(e)))
           .takeUntil(fromEvent(`end`).merge(fromEvent(`close`)))
           .defaultIfEmpty(`empty stream`)
           .multicast(new ReplaySubject()).refCount();
    streamObs.stream = pumped;
    streamObs.observable = streamObs;
    return streamObs;
}

function* combinations(_targets, _modules) {

    const targets = known(knownTargets, _targets || [`all`]);
    const modules = known(knownModules, _modules || [`all`]);

    if (_targets[0] === `all` && _modules[0] === `all`) {
        yield [`ts`, ``];
        yield [npmPkgName, ``];
    }        
    
    for (const format of modules) {
        for (const target of targets) {
            yield [target, format];
        }
    }

    function known(known, values) {
        return ~values.indexOf(`all`)
            ? known
            : Object.keys(
                values.reduce((map, arg) => ((
                    (known.indexOf(arg) !== -1) &&
                    (map[arg.toLowerCase()] = true)
                    || true) && map
                ), {})
            ).sort((a, b) => known.indexOf(a) - known.indexOf(b));
    }
}
    
module.exports = {

    mainExport, npmPkgName, npmOrgName, metadataFiles, packageJSONFields,

    knownTargets, knownModules, moduleFormatsToSkipCombosOf,
    ESKeywords, gCCLanguageNames, UMDSourceTargets, uglifyLanguageNames,

    taskName, packageName, tsconfigName, targetDir, combinations, observableFromStreams,
};