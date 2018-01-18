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

import { Data, NestedData } from './data';
import { Schema, DataType, Struct, IterableArrayLike } from './type';
import { flatbuffers } from 'flatbuffers';
import { StructView, RowView } from './vector/nested';
import { read, readAsync } from './ipc/reader/arrow';
import { View, Vector, createVector } from './vector';
import { isPromise, isAsyncIterable } from './util/compat';

import Long = flatbuffers.Long;

export class RecordBatch {
    public readonly numRows: number;
    public readonly schema: Schema;
    public readonly data: Data<DataType>[];
    public readonly columns: Vector<any>[];
    constructor(schema: Schema, numRows: Long | number, columnsOrData: (Data<DataType> | Vector)[]) {
        const data: Data<any>[] = new Array(columnsOrData.length);
        const columns: Vector<any>[] = new Array(columnsOrData.length);
        for (let index = -1, length = columnsOrData.length; ++index < length;) {
            const col: Data<any> | Vector = columnsOrData[index];
            if (col instanceof Vector) {
                data[index] = (columns[index] = col as Vector).data;
            } else {
                columns[index] = createVector(data[index] = col);
            }
        }
        this.data = data;
        this.schema = schema;
        this.columns = columns;
        this.numRows = typeof numRows === 'number' ? numRows : numRows.low;
    }
    public get numCols() { return this.columns.length; }
    public concat(...others: RecordBatch[]): RecordBatch {
        return new RecordBatch(
            this.schema,
            others.reduce((numRows, batch) => numRows + batch.numRows, this.numRows),
            others.reduce((columns, batch) =>
                columns.map((col, idx) => col.concat(batch.columns[idx])),
                this.columns
            )
        );
    }
}

export class Table {

    public static from = syncTableFromInputs;
    public static fromAsync = asyncTableFromInputs;
    public static empty() { return new Table(new Schema([]), []); }

    protected _view: View<any>;
    public readonly schema: Schema;
    public readonly columns: Vector<any>[];

    constructor(schema: Schema, columns: Vector<any>[]) {
        this.schema = schema;
        this.columns = columns;
        this._view = new StructView(
            new NestedData<Struct>(
                new Struct(schema.fields),
                this.numRows, new Uint8Array(0),
                columns.map((col) => col.data)
            ),
            columns
        );
    }
    public get numCols() { return this.columns.length; }
    public get numRows() { return this.columns[0].length; }
    public get(index: number) { return this._view.get(index); }
    public toArray(): IterableArrayLike<any> { return this._view.toArray(); }
    public [Symbol.iterator](): IterableIterator<RowView> {
        return this._view[Symbol.iterator]();
    }
    public select(...columnNames: string[]) {
        const fields = this.schema.fields;
        const namesToKeep = columnNames.reduce((xs, x) => (xs[x] = true) && xs, Object.create(null));
        return new Table(
            this.schema.select(...columnNames),
            this.columns.filter((_, index) => namesToKeep[fields[index].name])
        );
    }
    public rowsToString(separator = ' | '): TableToStringIterator {
        return new TableToStringIterator(tableRowsToString(this, separator));
    }
}

export function syncTableFromInputs<T extends Uint8Array | Buffer | string>(sources?: Iterable<T> | object | string) {
    let schema: Schema | undefined, columns: Vector[] = [];
    if (sources) {
        for (let recordBatch of read(sources)) {
            schema = schema || recordBatch.schema;
            columns = concatVectors(columns, recordBatch.columns);
        }
        return new Table(schema!, columns);
    }
    return Table.empty();
}

export async function* asyncTableFromInputs<T extends Uint8Array | Buffer | string>(sources?: Iterable<T> | AsyncIterable<T> | Promise<object | string> | object | string) {
    let columns: Vector[] = [];
    let schema: Schema | undefined;
    if (isAsyncIterable(sources)) {
        for await (let recordBatch of readAsync(sources)) {
            schema = schema || recordBatch.schema;
            columns = concatVectors(columns, recordBatch.columns);
        }
        return new Table(schema!, columns);
    } else if (isPromise(sources)) {
        return Table.from(await sources);
    } else if (sources) {
        return Table.from(sources);
    }
    return Table.empty();
}

export class TableToStringIterator implements IterableIterator<string> {
    constructor(private iterator: IterableIterator<string>) {}
    [Symbol.iterator]() { return this.iterator; }
    next(value?: any) { return this.iterator.next(value); }
    throw(error?: any) { return this.iterator.throw && this.iterator.throw(error) || { done: true, value: '' }; }
    return(value?: any) { return this.iterator.return && this.iterator.return(value) || { done: true, value: '' }; }
    pipe(stream: NodeJS.WritableStream) {
        let res: IteratorResult<string>;
        let write = () => {
            if (stream.writable) {
                do {
                    if ((res = this.next()).done) { break; }
                } while (stream.write(res.value + '\n', 'utf8'));
            }
            if (!res || !res.done) {
                stream.once('drain', write);
            } else if (!(stream as any).isTTY) {
                stream.end('\n');
            }
        };
        write();
    }
}

function *tableRowsToString(table: Table, separator = ' | ') {
    const fields = table.schema.fields;
    const header = ['row_id', ...fields.map((f) => `${f}`)].map(stringify);
    const maxColumnWidths = header.map(x => x.length);
    // Pass one to convert to strings and count max column widths
    for (let i = -1, n = table.numRows - 1; ++i < n;) {
        let val, row = [i, ...table.get(i)];
        for (let j = -1, k = row.length; ++j < k; ) {
            val = stringify(row[j]);
            maxColumnWidths[j] = Math.max(maxColumnWidths[j], val.length);
        }
    }
    yield header.map((x, j) => leftPad(x, ' ', maxColumnWidths[j])).join(separator);
    for (let i = -1, n = table.numRows; ++i < n;) {
        yield [i, ...table.get(i)]
            .map((x) => stringify(x))
            .map((x, j) => leftPad(x, ' ', maxColumnWidths[j]))
            .join(separator);
    }
}

function concatVectors(tableVectors: Vector<any>[], batchVectors: Vector<any>[]) {
    return tableVectors.length === 0 ? batchVectors : batchVectors.map((vec, i, _vs, col = tableVectors[i]) =>
        vec && col && col.concat(vec) || col || vec
    ) as Vector<any>[];
}

function leftPad(str: string, fill: string, n: number) {
    return (new Array(n + 1).join(fill) + str).slice(-1 * n);
}

function stringify(x: any) {
    return typeof x === 'string' ? `"${x}"` : ArrayBuffer.isView(x) ? `[${x}]` : JSON.stringify(x);
}
