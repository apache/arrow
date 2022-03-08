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

import * as Arrow from 'apache-arrow';
const { BitIterator, getBool } = Arrow.util;

describe('Bits', () => {
    test('BitIterator produces correct bits for single byte', () => {
        const byte = new Uint8Array([0b1111_0000]);
        expect([...new BitIterator(byte, 0, 8, null, getBool)]).toEqual(
            [false, false, false, false, true, true, true, true]);

        expect([...new BitIterator(byte, 2, 5, null, getBool)]).toEqual(
            [false, false, true, true, true]);
    });

    test('BitIterator produces correct bits for multiple bytes', () => {
        const byte = new Uint8Array([0b1111_0000, 0b1010_1010]);
        expect([...new BitIterator(byte, 0, 16, null, getBool)]).toEqual(
            [false, false, false, false, true, true, true, true,
             false, true, false, true, false, true, false, true]);

        expect([...new BitIterator(byte, 2, 11, null, getBool)]).toEqual(
            [false, false, true, true, true, true,
             false, true, false, true, false]);
    });
});
