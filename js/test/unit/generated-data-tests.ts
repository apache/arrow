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

import '../jest-extensions.js';
import * as generate from '../generate-test-data.js';
import { validateTable } from './generated-data-validators.js';
import { validateVector } from './generated-data-validators.js';
import { validateRecordBatch } from './generated-data-validators.js';

describe('Generated Test Data', () => {
    describe('Table', () => { validateTable(generate.table([100, 150, 75])); });
    describe('RecordBatch', () => { validateRecordBatch(generate.recordBatch()); });
    describe('Null', () => { validateVector(generate.null_()); });
    describe('Bool', () => { validateVector(generate.bool()); });
    describe('Int8', () => { validateVector(generate.int8()); });
    describe('Int16', () => { validateVector(generate.int16()); });
    describe('Int32', () => { validateVector(generate.int32()); });
    describe('Int64', () => { validateVector(generate.int64()); });
    describe('Uint8', () => { validateVector(generate.uint8()); });
    describe('Uint16', () => { validateVector(generate.uint16()); });
    describe('Uint32', () => { validateVector(generate.uint32()); });
    describe('Uint64', () => { validateVector(generate.uint64()); });
    describe('Float16', () => { validateVector(generate.float16()); });
    describe('Float32', () => { validateVector(generate.float32()); });
    describe('Float64', () => { validateVector(generate.float64()); });
    describe('Utf8', () => { validateVector(generate.utf8()); });
    describe('Binary', () => { validateVector(generate.binary()); });
    describe('FixedSizeBinary', () => { validateVector(generate.fixedSizeBinary()); });
    describe('DateDay', () => { validateVector(generate.dateDay()); });
    describe('DateMillisecond', () => { validateVector(generate.dateMillisecond()); });
    describe('TimestampSecond', () => { validateVector(generate.timestampSecond()); });
    describe('TimestampMillisecond', () => { validateVector(generate.timestampMillisecond()); });
    describe('TimestampMicrosecond', () => { validateVector(generate.timestampMicrosecond()); });
    describe('TimestampNanosecond', () => { validateVector(generate.timestampNanosecond()); });
    describe('TimeSecond', () => { validateVector(generate.timeSecond()); });
    describe('TimeMillisecond', () => { validateVector(generate.timeMillisecond()); });
    describe('TimeMicrosecond', () => { validateVector(generate.timeMicrosecond()); });
    describe('TimeNanosecond', () => { validateVector(generate.timeNanosecond()); });
    describe('Decimal', () => { validateVector(generate.decimal()); });
    describe('List', () => { validateVector(generate.list()); });
    describe('Struct', () => { validateVector(generate.struct()); });
    describe('DenseUnion', () => { validateVector(generate.denseUnion()); });
    describe('SparseUnion', () => { validateVector(generate.sparseUnion()); });
    describe('Dictionary', () => { validateVector(generate.dictionary()); });
    describe('IntervalDayTime', () => { validateVector(generate.intervalDayTime()); });
    describe('IntervalYearMonth', () => { validateVector(generate.intervalYearMonth()); });
    describe('FixedSizeList', () => { validateVector(generate.fixedSizeList()); });
    describe('Map', () => { validateVector(generate.map()); });
});
