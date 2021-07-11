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

module.exports = {
    "verbose": false,
    "testEnvironment": "node",
    "globals": {
      "ts-jest": {
        "diagnostics": false,
        "tsconfig": "test/tsconfig.json",
        "useESM": true
      }
    },
    "rootDir": ".",
    "roots": [
      "<rootDir>/test/"
    ],
    "preset": "ts-jest/presets/default-esm",
    "moduleFileExtensions": [
      "js",
      "ts"
    ],
    "coverageReporters": [
      "lcov", "json"
    ],
    "coveragePathIgnorePatterns": [
      "fb\\/(File|Message|Schema|Tensor)\\.(js|ts)$",
      "test\\/.*\\.(ts|js)$",
      "/node_modules/"
    ],
    "transform": {
      "^.+\\.js$": "ts-jest",
      "^.+\\.ts$": "ts-jest"
    },
    "transformIgnorePatterns": [
      "/targets/(es5|es2015|esnext|apache-arrow)/",
      "/node_modules/(?!@openpgp/web-stream-tools)/"
    ],
    "testRegex": "(.*(-|\\.)(test|spec)s?)\\.(ts|js)$",
    "testMatch": null,
    "moduleNameMapper": {
      "^apache-arrow$": "<rootDir>/src/Arrow.node",
      "^apache-arrow(.*)": "<rootDir>/src$1",
      "flatbuffers": "flatbuffers/js/flatbuffers.mjs"
    }
};
