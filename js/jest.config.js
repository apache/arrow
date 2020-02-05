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
    "reporters": [
      "jest-silent-reporter"
    ],
    "testEnvironment": "node",
    "globals": {
      "ts-jest": {
        "diagnostics": false,
        "tsConfig": "test/tsconfig.json"
      }
    },
    "roots": [
      "<rootDir>/test/"
    ],
    "moduleFileExtensions": [
      "js",
      "ts",
      "tsx"
    ],
    "coverageReporters": [
      "lcov"
    ],
    "coveragePathIgnorePatterns": [
      "fb\\/(File|Message|Schema|Tensor)\\.(js|ts)$",
      "test\\/.*\\.(ts|tsx|js)$",
      "/node_modules/"
    ],
    "transform": {
      "^.+\\.jsx?$": "ts-jest",
      "^.+\\.tsx?$": "ts-jest"
    },
    "transformIgnorePatterns": [
      "/node_modules/(?!web-stream-tools).+\\.js$"
    ],
    "testRegex": "(.*(-|\\.)(test|spec)s?)\\.(ts|tsx|js)$",
    "preset": "ts-jest",
    "testMatch": null
};
