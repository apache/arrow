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

export const LOWER = 'abcdefghijklmnopqrstuvwxyz';
export const UPPER = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
export const NUMBER = '0123456789';
export const SPECIAL = '~!@#$%^&()_+-={}[];\',.';

export const ALL = LOWER + UPPER + NUMBER + SPECIAL;

/**
 * Generate random string of specified `length` for the given `pattern`.
 *
 * @param `pattern` The pattern to use for generating the random string.
 * @param `length` The length of the string to generate.
 * @param `options`
 */
export function randomString(length: number, characters: string = `${LOWER + NUMBER}_`) {
    let result = '';

    while (length--) {
        result += characters.charAt(Math.floor(Math.random() * characters.length));
    }
    return result;
}


10;

