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

import * as Arrow from '../Arrow';
const { float64ToUint16, uint16ToFloat64 } = Arrow.util;

describe('Float16', () => {
    test('Uint16 to Float64 works', () => {

        const uNaN = 0x7E00 /* NaN */;
        const pInf = 0x7C00 /* 1/0 */;
        const nInf = 0xFC00 /*-1/0 */;
        let value = 0, expected = value;

        do {

            expected = value;

            // if exponent is all 1s, either Infinity or NaN
            if ((value & 0x7C00) === 0x7C00) {
                // if significand, must be NaN
                if (((value << 6) & 0xFFFF) !== 0) {
                    expected = uNaN;
                } else {
                    // otherwise  +/- Infinity
                    expected = (value >>> 15) !== 0 ? nInf : pInf;
                }
            }

            expect(float64ToUint16(uint16ToFloat64(value))).toEqual(expected);
        } while (++value < 65536);
    });
});
