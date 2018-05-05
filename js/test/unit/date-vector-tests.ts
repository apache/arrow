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

import Arrow from '../Arrow';
import { DateVector } from '../../src/vector';
const { Table } = Arrow;

describe(`DateVector`, () => {
    it('returns days since the epoch as correct JS Dates', () => {
        const table = Table.from(test_data);
        const date32 = table.getColumnAt(0) as DateVector;
        const expectedMillis = expectedMillis32();
        for (const date of date32) {
            const millis = expectedMillis.shift();
            expect(date).toEqual(millis === null ? null : new Date(millis!));
        }
    });
    it('returns millisecond longs since the epoch as correct JS Dates', () => {
        const table = Table.from(test_data);
        const date64 = table.getColumnAt(1) as DateVector;
        const expectedMillis = expectedMillis64();
        for (const date of date64) {
            const millis = expectedMillis.shift();
            expect(date).toEqual(millis === null ? null : new Date(millis!));
        }
    });
    it('converts days since the epoch to milliseconds', () => {
        const table = Table.from(test_data);
        const date32 = table.getColumnAt(0) as DateVector;
        const expectedMillis = expectedMillis32();
        for (const timestamp of date32.asEpochMilliseconds()) {
            expect(timestamp).toEqual(expectedMillis.shift());
        }
    });
    it('converts millisecond longs since the epoch to millisecond ints', () => {
        const table = Table.from(test_data);
        const date64 = table.getColumnAt(1) as DateVector;
        const expectedMillis = expectedMillis64();
        for (const timestamp of date64.asEpochMilliseconds()) {
            expect(timestamp).toEqual(expectedMillis.shift());
        }
    });
});

const expectedMillis32 = () => [
    165247430400000, 34582809600000, 232604524800000, null,
    199808812800000, 165646771200000, 209557238400000, null
];

const expectedMillis64 = () => [
    27990830234011, -41278585914325, 12694624797111,
    null, null, 10761360520213, null, 1394015437000
];

const test_data = {
    'schema': {
        'fields': [
            {
                'name': 'f0',
                'type': {
                    'name': 'date',
                    'unit': 'DAY'
                },
                'nullable': true,
                'children': []
            },
            {
                'name': 'f1',
                'type': {
                    'name': 'date',
                    'unit': 'MILLISECOND'
                },
                'nullable': true,
                'children': []
            }
        ]
    },
    'batches': [
        {
            'count': 8,
            'columns': [
                {
                    'name': 'f0',
                    'count': 8,
                    'VALIDITY': [1, 1, 1, 0, 1, 1, 1, 0],
                    'DATA': [1912586, 400264, 2692182, 2163746, 2312602, 1917208, 2425431]
                },
                {
                    'name': 'f1',
                    'count': 8,
                    'VALIDITY': [1, 1, 1, 0, 0, 1, 0, 1],
                    'DATA': [
                        27990830234011,
                        -41278585914325,
                        12694624797111,
                        -38604948562547,
                        -37802308043516,
                        10761360520213,
                        -25129181633384,
                        1394015437000 // <-- the tricky one
                    ]
                }
            ]
        }
    ]
};
