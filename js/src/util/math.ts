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

/**
 * Convert uint16 (logically a float16) to a JS float64.
 * Adapted from https://gist.github.com/zed/59a413ae2ed4141d2037
 * @param n {number} the uint16 to convert
 * @private
 * @ignore
 */
export function uint16ToFloat64(n: number) {

    // magic numbers:
    // 31 = b011111
    // 1024 = 2 ** 10

    const sign = n >> 15;
    const exponent = (n >> 10) & 31;
    const fraction = n & (1024 - 1);

    if (exponent === 0) {
        if (fraction === 0) {
            return sign ? -0 : 0;
        }
        return ((-1) ** sign) * (fraction / 1024) * (2 ** -14);
    }
    if (exponent === 31) {
        if (fraction === 0) {
            return sign ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY;
        }
        return NaN;
    }
    return ((-1) ** sign) * (1 + fraction / 1024) * (2 ** (exponent - 15));
}

/**
 * Convert a float64 to uint16 (assuming the float64 is logically a float16).
 * Adapted from https://gist.github.com/zed/59a413ae2ed4141d2037
 * @param n {number} The JS float64 to convert
 * @private
 * @ignore
 */
export function float64ToUint16(n: number) {

    // magic numbers:
    // 31 = b011111
    // 1024 = 2 ** 10
    // 16777216 = 2 ** 24
    // 31744 = b0111110000000000 (positive infinity)
    // 64512 = b1111110000000000 (negative infinity)

    // Anything with exponent == 31 and mantissa > 0 is NaN,
    // 65535 is b1111111111111111, the max 16 bit value with exponent 31
    if (n !== n) { return 65535; }
    // negative zero (-0) is uint16_t 32768
    if (n === 0 && (1 / n < 0)) { return 32768; }

    const sign = Math.sign(n) < 0;

    if (!isFinite(n)) {
        return sign ? 64512 : 31744;
    }

    let [mantissa, exponent] = frexp(n);
    if (mantissa === 0 && exponent === 0) { return sign ? -0 : 0; }
    if (mantissa !== mantissa || exponent !== exponent) { return 65535; }

    mantissa = Math.trunc((2 * Math.abs(mantissa) - 1) * 1024); // round toward zero
    exponent = exponent + 14;

    if (exponent <= 0) { // subnormal
        mantissa = (16777216 * Math.abs(n) + .5) | 0; // round
        exponent = 0;
    } else if (exponent >= 31) {
        return sign ? 64512 : 31744;
    }

    return (+sign << 15) | (exponent << 10) | mantissa;
}

/**
 * Extract the mantissa and exponent from a JS float64, such that `f = mantissa * 2^exponent`.
 * Adapted from http://croquetweak.blogspot.com/2014/08/deconstructing-floats-frexp-and-ldexp.html
 * @returns `Float64Array` of the mantissa and exponent respectively
 * @private
 * @ignore
 */
const frexp = (() => {

    // Float64Array used to store and return the [mantissa, exponent] of a JS float64
    const f64 = new Float64Array(2);
    // Uint32Array used to store the [lo, hi] bits of a JS float64
    const u32 = new Uint32Array(f64.buffer);

    return frexp;

    function frexp(f: number) {
        f64[0] = f;
        f64[1] = 0;
        if (f === 0) { return f64; }
        // extract the unsigned exponent bits
        let exponent = (u32[1] >>> 20) & 0x7FF;
        // if the exponent is 0, the rest of the 53 bits are being used to represent
        // a decimal number between -1.0 < f < 1.0.
        if (exponent === 0) { // subnormal
            f64[0] = f * Math.pow(2, 64);
            exponent = ((u32[1] >>> 20) & 0x7FF) - 64;
        }
        // apply the bias
        f64[1] = exponent - 1022;
        // compute the mantissa
        f64[0] = ldexp(f, -f64[1]);
        return f64;
    }

    function ldexp(f: number, exp: number) {
        // Multiply the mantissa by the exponent in stages. The exponent can
        // be as high as 1073, and 2^1073 is larger than max float at 2^1023
        let n = Math.min(3, Math.ceil(Math.abs(exp) / 1023));
        for (let i = -1; ++i < n;) {
            f *= (2 ** ((exp + i) / n | 0));
        }
        return f;
    }
})();
