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
    env: {
        browser: true,
        es6: true,
        node: true,
    },
    parser: "@typescript-eslint/parser",
    parserOptions: {
        project: "tsconfig.json",
        sourceType: "module",
        ecmaVersion: 2020,
    },
    plugins: ["@typescript-eslint", "jest", "unicorn"],
    extends: [
        "eslint:recommended",
        "plugin:unicorn/recommended",
        "plugin:jest/recommended",
        "plugin:jest/style",
        "plugin:@typescript-eslint/recommended",
    ],
    rules: {
        "@typescript-eslint/member-delimiter-style": [
            "error",
            {
                multiline: {
                    delimiter: "semi",
                    requireLast: true,
                },
                singleline: {
                    delimiter: "semi",
                    requireLast: false,
                },
            },
        ],
        "@typescript-eslint/no-namespace": ["error", { "allowDeclarations": true }],
        "@typescript-eslint/no-require-imports": "error",
        "@typescript-eslint/no-var-requires": "off",  // handled by rule above
        "@typescript-eslint/quotes": [
            "error",
            "single",
            {
                avoidEscape: true,
                allowTemplateLiterals: true
            },
        ],
        "@typescript-eslint/semi": ["error", "always"],
        "@typescript-eslint/type-annotation-spacing": "error",
        "@typescript-eslint/indent": "off",
        "@typescript-eslint/no-empty-function": "off",
        "@typescript-eslint/no-unused-expressions": "off",
        "@typescript-eslint/no-use-before-define": "off",
        "@typescript-eslint/explicit-module-boundary-types": "off",
        "@typescript-eslint/no-explicit-any": "off",
        "@typescript-eslint/no-misused-new": "off",
        "@typescript-eslint/ban-ts-comment": "off",
        "@typescript-eslint/no-non-null-assertion": "off",
        "@typescript-eslint/no-unused-vars": "off",  // ts already takes care of this

        "prefer-const": ["error", {
            "destructuring": "all"
        }],
        "curly": ["error", "multi-line"],
        "brace-style": ["error", "1tbs", { "allowSingleLine": true }],
        "eol-last": "error",
        "no-multiple-empty-lines": "error",
        "no-trailing-spaces": "error",
        "no-var": "error",
        "no-empty": "off",
        "no-cond-assign": "off",

        "unicorn/catch-error-name": "off",
        "unicorn/no-nested-ternary": "off",
        "unicorn/no-new-array": "off",
        "unicorn/no-null": "off",
        "unicorn/empty-brace-spaces": "off",
        "unicorn/no-zero-fractions": "off",
        "unicorn/prevent-abbreviations": "off",
        "unicorn/prefer-module": "off",
        "unicorn/numeric-separators-style": "off",
        "unicorn/prefer-spread": "off",
        "unicorn/filename-case": "off",
        "unicorn/prefer-export-from": "off",
        "unicorn/prefer-switch": "off",
        "unicorn/prefer-node-protocol": "off",
        "unicorn/text-encoding-identifier-case": "off",
        "unicorn/prefer-top-level-await": "off",

        "unicorn/consistent-destructuring": "warn",
        "unicorn/no-array-reduce": ["warn", { "allowSimpleOperations": true }],
        "unicorn/no-await-expression-member": "warn",
        "unicorn/no-useless-undefined": "warn",
        "unicorn/consistent-function-scoping": "warn",
        "unicorn/prefer-math-trunc": "warn",
        "unicorn/no-negated-condition": "off",
        "unicorn/switch-case-braces": "off",
        "unicorn/no-typeof-undefined": "off",
    },
};
