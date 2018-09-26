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

import Arrow from '../Arrow';
import { getSingleRecordBatchTable } from './table-tests';
const { Table, RecordBatch } = Arrow;

describe('Table.serialize()', () => {
    test(`Serializes sliced RecordBatches`, () => {

        const table = getSingleRecordBatchTable();
        const batch = table.batches[0], half = batch.length / 2 | 0;

        // First compare what happens when slicing from the batch level
        let [batch1, batch2] = [batch.slice(0, half), batch.slice(half)];

        compareBatchAndTable(table,    0, batch1, Table.from(new Table(batch1).serialize()));
        compareBatchAndTable(table, half, batch2, Table.from(new Table(batch2).serialize()));

        // Then compare what happens when creating a RecordBatch by slicing each child individually
        batch1 = new RecordBatch(batch1.schema, batch1.length, batch1.schema.fields.map((_, i) => {
            return batch.getChildAt(i)!.slice(0, half);
        }));

        batch2 = new RecordBatch(batch2.schema, batch2.length, batch2.schema.fields.map((_, i) => {
            return batch.getChildAt(i)!.slice(half);
        }));

        compareBatchAndTable(table,    0, batch1, Table.from(new Table(batch1).serialize()));
        compareBatchAndTable(table, half, batch2, Table.from(new Table(batch2).serialize()));
    });
});

function compareBatchAndTable(source: Table, offset: number, batch: RecordBatch, table: Table) {
    expect(batch.length).toEqual(table.length);
    expect(table.numCols).toEqual(source.numCols);
    expect(batch.numCols).toEqual(source.numCols);
    for (let i = -1, n = source.numCols; ++i < n;) {
        const v0 = source.getColumnAt(i)!.slice(offset, offset + batch.length);
        const v1 = batch.getChildAt(i);
        const v2 = table.getColumnAt(i);
        const name = source.schema.fields[i].name;
        (expect([v1, `batch`, name]) as any).toEqualVector([v0, `source`]);
        (expect([v2, `table`, name]) as any).toEqualVector([v0, `source`]);
    }
}
