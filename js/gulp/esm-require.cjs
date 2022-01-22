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

const esm = require("esm");

const esmRequire = esm(module, {
    mode: `auto`,
    cjs: {
        /* A boolean for storing ES modules in require.cache. */
        cache: true,
        /* A boolean for respecting require.extensions in ESM. */
        extensions: true,
        /* A boolean for __esModule interoperability. */
        interop: true,
        /* A boolean for importing named exports of CJS modules. */
        namedExports: true,
        /* A boolean for following CJS path rules in ESM. */
        paths: true,
        /* A boolean for __dirname, __filename, and require in ESM. */
        vars: true,
    }
});

module.exports = esmRequire;
