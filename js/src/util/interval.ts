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

export interface IntervalMonthDayNanoObject<StringifyNano extends boolean = false> {
    months: number;
    days: number;
    nanoseconds: StringifyNano extends true ? string : bigint | number;
}

export interface IntervalDayTimeObject {
    days: number;
    milliseconds: number;
}

export function toIntervalDayTimeInt32Array(objects: IntervalDayTimeObject[]) {
    const length = objects.length;
    const array = new Int32Array(length * 2);
    for (let oi = 0, ai = 0; oi < length; oi++) {
        const interval = objects[oi];
        array[ai++] = interval['days'] ?? 0;
        array[ai++] = interval['milliseconds'] ?? 0;
    }
    return array;
}

export function toIntervalMonthDayNanoInt32Array(objects: Partial<IntervalMonthDayNanoObject>[]) {
    const length = objects.length;
    const data = new Int32Array(length * 4);
    for (let oi = 0, ai = 0; oi < length; oi++) {
        const interval = objects[oi];
        data[ai++] = interval['months'] ?? 0;
        data[ai++] = interval['days'] ?? 0;
        const nanoseconds = interval['nanoseconds'];
        if (nanoseconds) {
            data[ai++] = Number(BigInt(nanoseconds) & BigInt(0xFFFFFFFF));
            data[ai++] = Number(BigInt(nanoseconds) >> BigInt(32));
        } else {
            ai += 2;
        }
    }
    return data;
}

export function toIntervalDayTimeObjects(array: Int32Array): IntervalDayTimeObject[] {
    const length = array.length;
    const objects = new Array<IntervalDayTimeObject>(length / 2);
    for (let ai = 0, oi = 0; ai < length; ai += 2) {
        objects[oi++] = {
            'days': array[ai],
            'milliseconds': array[ai + 1]
        };
    }
    return objects;
}

/** @ignore */
export function toIntervalMonthDayNanoObjects<StringifyNano extends boolean>(
    array: Int32Array, stringifyNano: StringifyNano
): IntervalMonthDayNanoObject<StringifyNano>[] {
    const length = array.length;
    const objects = new Array<IntervalMonthDayNanoObject<StringifyNano>>(length / 4);
    for (let ai = 0, oi = 0; ai < length; ai += 4) {
        const nanoseconds = (BigInt(array[ai + 3]) << BigInt(32)) | BigInt(array[ai + 2] >>> 0);
        objects[oi++] = {
            'months': array[ai],
            'days': array[ai + 1],
            'nanoseconds': (stringifyNano ? `${nanoseconds}` : nanoseconds) as IntervalMonthDayNanoObject<StringifyNano>['nanoseconds'],
        };
    }
    return objects;
}
