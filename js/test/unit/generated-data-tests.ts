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

import '../jest-extensions';
import * as generate from '../generate-test-data';
import { validateTable, validateRecordBatch, validateVector } from './generated-data-validators';

describe('Generated Test Data', () => {
    describe('Table',                      () => validateTable(generate.table([100, 150, 75])).run());
    describe('RecordBatch',                () => validateRecordBatch(generate.recordBatch()).run());
    describe('NullVector',                 () => validateVector(generate.null_()).run());
    describe('BoolVector',                 () => validateVector(generate.bool()).run());
    describe('Int8Vector',                 () => validateVector(generate.int8()).run());
    describe('Int16Vector',                () => validateVector(generate.int16()).run());
    describe('Int32Vector',                () => validateVector(generate.int32()).run());
    describe('Int64Vector',                () => validateVector(generate.int64()).run());
    describe('Uint8Vector',                () => validateVector(generate.uint8()).run());
    describe('Uint16Vector',               () => validateVector(generate.uint16()).run());
    describe('Uint32Vector',               () => validateVector(generate.uint32()).run());
    describe('Uint64Vector',               () => validateVector(generate.uint64()).run());
    describe('Float16Vector',              () => validateVector(generate.float16()).run());
    describe('Float32Vector',              () => validateVector(generate.float32()).run());
    describe('Float64Vector',              () => validateVector(generate.float64()).run());
    describe('Utf8Vector',                 () => validateVector(generate.utf8()).run());
    describe('BinaryVector',               () => validateVector(generate.binary()).run());
    describe('FixedSizeBinaryVector',      () => validateVector(generate.fixedSizeBinary()).run());
    describe('DateDayVector',              () => validateVector(generate.dateDay()).run());
    describe('DateMillisecondVector',      () => validateVector(generate.dateMillisecond()).run());
    describe('TimestampSecondVector',      () => validateVector(generate.timestampSecond()).run());
    describe('TimestampMillisecondVector', () => validateVector(generate.timestampMillisecond()).run());
    describe('TimestampMicrosecondVector', () => validateVector(generate.timestampMicrosecond()).run());
    describe('TimestampNanosecondVector',  () => validateVector(generate.timestampNanosecond()).run());
    describe('TimeSecondVector',           () => validateVector(generate.timeSecond()).run());
    describe('TimeMillisecondVector',      () => validateVector(generate.timeMillisecond()).run());
    describe('TimeMicrosecondVector',      () => validateVector(generate.timeMicrosecond()).run());
    describe('TimeNanosecondVector',       () => validateVector(generate.timeNanosecond()).run());
    describe('DecimalVector',              () => validateVector(generate.decimal()).run());
    describe('ListVector',                 () => validateVector(generate.list()).run());
    describe('StructVector',               () => validateVector(generate.struct()).run());
    describe('DenseUnionVector',           () => validateVector(generate.denseUnion()).run());
    describe('SparseUnionVector',          () => validateVector(generate.sparseUnion()).run());
    describe('DictionaryVector',           () => validateVector(generate.dictionary()).run());
    describe('IntervalDayTimeVector',      () => validateVector(generate.intervalDayTime()).run());
    describe('IntervalYearMonthVector',    () => validateVector(generate.intervalYearMonth()).run());
    describe('FixedSizeListVector',        () => validateVector(generate.fixedSizeList()).run());
    describe('MapVector',                  () => validateVector(generate.map()).run());
});
