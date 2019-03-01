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

import '../../jest-extensions';
import * as generate from '../../generate-test-data';
import {
    Table, Schema, Field, DataType, Dictionary, Int32, Float32, Utf8, Null
} from '../../Arrow';

const toSchema = (...xs: [string, DataType][]) => new Schema(xs.map((x) => new Field(...x)));
const schema1 = toSchema(['a', new Int32()], ['b', new Float32()], ['c', new Dictionary(new Utf8(), new Int32())]);
const schema2 = toSchema(['d', new Int32()], ['e', new Float32()], ['f', new Utf8()]);
const nullSchema = new Schema([new Field('null', new Null())]);

schema1.metadata.set('foo', 'bar');

function createTable<T extends { [key: string]: DataType } = any>(schema: Schema<T>, chunkLengths: number[]) {
    return generate.table(chunkLengths, schema).table;
}

describe('Table#serialize()', () => {

    test(`Table#empty round-trips through serialization`, () => {
        const source = Table.empty();
        source.schema.metadata.set('foo', 'bar');
        expect(source.length).toBe(0);
        expect(source.numCols).toBe(0);
        const result = Table.from(source.serialize());
        expect(result).toEqualTable(source);
        expect(result.schema.metadata.get('foo')).toEqual('bar');
    });

    test(`Schema metadata round-trips through serialization`, () => {
        const source = createTable(schema1, [20]);
        expect(source.length).toBe(20);
        expect(source.numCols).toBe(3);
        const result = Table.from(source.serialize());
        expect(result).toEqualTable(source);
        expect(result.schema.metadata.get('foo')).toEqual('bar');
    });

    test(`Table#assign an empty Table to a Table with a zero-length Null column round-trips through serialization`, () => {
        const table1 = new Table(nullSchema);
        const table2 = Table.empty();
        const source = table1.assign(table2);
        expect(source.length).toBe(0);
        expect(source.numCols).toBe(1);
        const result = Table.from(source.serialize());
        expect(result).toEqualTable(source);
    });

    const chunkLengths = [] as number[];
    for (let i = -1; ++i < 3;) {
        chunkLengths[i * 2] = (Math.random() * 100) | 0;
        chunkLengths[i * 2 + 1] = 0;
        const table = <T extends { [key: string]: DataType } = any>(schema: Schema<T>) => createTable(schema, chunkLengths);
        test(`Table#select round-trips through serialization`, () => {
            const source = table(schema1).select('a', 'c');
            expect(source.numCols).toBe(2);
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
        });
        test(`Table#selectAt round-trips through serialization`, () => {
            const source = table(schema1).selectAt(0, 2);
            expect(source.numCols).toBe(2);
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
        });
        test(`Table#assign round-trips through serialization`, () => {
            const source = table(schema1).assign(table(schema2));
            expect(source.numCols).toBe(6);
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
            expect(result.schema.metadata.get('foo')).toEqual('bar');
        });
        test(`Table#assign with an empty table round-trips through serialization`, () => {
            const table1 = table(schema1);
            const source = table1.assign(Table.empty());
            expect(source.numCols).toBe(table1.numCols);
            expect(source.length).toBe(table1.length);
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
            expect(result.schema.metadata.get('foo')).toEqual('bar');
        });
        test(`Table#assign with a zero-length Null column round-trips through serialization`, () => {
            const table1 = new Table(nullSchema);
            const table2 = table(schema1);
            const source = table1.assign(table2);
            expect(source.length).toBe(table2.length);
            expect(source.numCols).toBe(4);
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
            expect(result.schema.metadata.get('foo')).toEqual('bar');
        });
        test(`Table#assign with different lengths and number of chunks round-trips through serialization`, () => {
            const table1 = table(schema1);
            const table2 = createTable(schema2, [102, 4, 10, 97, 10, 2, 4]);
            const source = table1.assign(table2);
            expect(source.numCols).toBe(6);
            expect(source.length).toBe(Math.max(table1.length, table2.length));
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
            expect(result.schema.metadata.get('foo')).toEqual('bar');
        });
        test(`Table#select with Table#assign the result of Table#selectAt round-trips through serialization`, () => {
            const table1 = table(schema1);
            const table2 = table(schema2);
            const source = table1.select('a', 'c').assign(table2.selectAt(2));
            expect(source.numCols).toBe(3);
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
            expect(result.schema.metadata.get('foo')).toEqual('bar');
        });
        test(`Table#slice round-trips through serialization`, () => {
            const table1 = table(schema1);
            const length = table1.length;
            const [begin, end] = [length * .25, length * .75].map((x) => x | 0);
            const source = table1.slice(begin, end);
            expect(source.numCols).toBe(3);
            expect(source.length).toBe(end - begin);
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
            expect(result.schema.metadata.get('foo')).toEqual('bar');
        });
        test(`Table#concat of two slices round-trips through serialization`, () => {
            const table1 = table(schema1);
            const length = table1.length;
            const [begin1, end1] = [length * .10, length * .20].map((x) => x | 0);
            const [begin2, end2] = [length * .80, length * .90].map((x) => x | 0);
            const slice1 = table1.slice(begin1, end1);
            const slice2 = table1.slice(begin2, end2);
            const source = slice1.concat(slice2);
            expect(slice1.length).toBe(end1 - begin1);
            expect(slice2.length).toBe(end2 - begin2);
            expect(source.length).toBe((end1 - begin1) + (end2 - begin2));
            [slice1, slice2, source].forEach((x) => expect(x.numCols).toBe(3));
            const result = Table.from(source.serialize());
            expect(result).toEqualTable(source);
            expect(result.schema.metadata.get('foo')).toEqual('bar');
        });
    }
});
