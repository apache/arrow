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

type EncodedIntervalMonthDayNano = {
    months: number;
    days: number;
    nanoseconds: bigint | number;
};

type EncodedIntervalDayTime = {
    days: number;
    milliseconds: number;
};

export function encodeIntervalDayTime(intervalArray: EncodedIntervalDayTime[]) {
    const length = intervalArray.length;
    const data = new Int32Array(length * 2);
    const intervalLength = intervalArray.length;
    for (let i = 0, ptr = 0; i < intervalLength; i++) {
        const interval = intervalArray[i];
        data[ptr++] = interval.days ?? 0;
        data[ptr++] = interval.milliseconds ?? 0;
    }
    return data;
}

export function encodeIntervalMonthDayNano(
    intervalArray: EncodedIntervalMonthDayNano[]
) {
    const length = intervalArray.length;
    const data = new Int32Array(length * 4);
    const intervalLength = intervalArray.length;
    for (let i = 0, ptr = 0; i < intervalLength; i++) {
        const interval = intervalArray[i];
        const nanoseconds = new Int32Array(
            new BigInt64Array([BigInt(interval.nanoseconds ?? 0)]).buffer
        );
        data[ptr++] = interval.months ?? 0;
        data[ptr++] = interval.days ?? 0;
        data[ptr++] = nanoseconds[0];
        data[ptr++] = nanoseconds[1];
    }
    return data;
}
