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

import { Table } from '../table';
import { Vector } from '../vector';
import { IntVector } from '../vector/int';
import { Field, Schema } from '../schema';
import { Vector as V } from '../interfaces';
import { Predicate, Col } from './predicate';
import { RecordBatch } from '../recordbatch';
import { DataType, Int, Struct, Dictionary } from '../type';

/** @ignore */
export type BindFunc = (batch: RecordBatch) => void;
/** @ignore */
export type NextFunc = (idx: number, batch: RecordBatch) => void;

Table.prototype.countBy = function(this: Table, name: Col | string) { return new DataFrame(this.chunks).countBy(name); };
Table.prototype.scan = function(this: Table, next: NextFunc, bind?: BindFunc) { return new DataFrame(this.chunks).scan(next, bind); };
Table.prototype.filter = function(this: Table, predicate: Predicate): FilteredDataFrame { return new DataFrame(this.chunks).filter(predicate); };

export class DataFrame<T extends { [key: string]: DataType } = any> extends Table<T> {
    public filter(predicate: Predicate): FilteredDataFrame<T> {
        return new FilteredDataFrame<T>(this.chunks, predicate);
    }
    public scan(next: NextFunc, bind?: BindFunc) {
        const batches = this.chunks, numBatches = batches.length;
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            if (bind) { bind(batch); }
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                next(index, batch);
            }
        }
    }
    public countBy(name: Col | string) {
        const batches = this.chunks, numBatches = batches.length;
        const count_by = typeof name === 'string' ? new Col(name) : name as Col;
        // Assume that all dictionary batches are deltas, which means that the
        // last record batch has the most complete dictionary
        count_by.bind(batches[numBatches - 1]);
        const vector = count_by.vector as V<Dictionary>;
        if (!DataType.isDictionary(vector.type)) {
            throw new Error('countBy currently only supports dictionary-encoded columns');
        }

        const countByteLength = Math.ceil(Math.log(vector.dictionary.length) / Math.log(256));
        const CountsArrayType = countByteLength == 4 ? Uint32Array :
                                countByteLength >= 2 ? Uint16Array : Uint8Array;

        const counts = new CountsArrayType(vector.dictionary.length);
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            // rebind the countBy Col
            count_by.bind(batch);
            const keys = (count_by.vector as V<Dictionary>).indices;
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                let key = keys.get(index);
                if (key !== null) { counts[key]++; }
            }
        }
        return new CountByResult(vector.dictionary, IntVector.from(counts));
    }
}

export class CountByResult<T extends DataType = any, TCount extends Int = Int> extends Table<{ values: T,  counts: TCount }> {
    constructor(values: Vector<T>, counts: V<TCount>) {
        type R = { values: T, counts: TCount };
        const schema = new Schema<R>([
            new Field('values', values.type),
            new Field('counts', counts.type)
        ]);
        super(new RecordBatch<R>(schema, counts.length, [values, counts]));
    }
    public toJSON(): Object {
        const values = this.getColumnAt(0)!;
        const counts = this.getColumnAt(1)!;
        const result = {} as { [k: string]: number | null };
        for (let i = -1; ++i < this.length;) {
            result[values.get(i)] = counts.get(i);
        }
        return result;
    }
}

export class FilteredDataFrame<T extends { [key: string]: DataType } = any> extends DataFrame<T> {
    private _predicate: Predicate;
    constructor (batches: RecordBatch<T>[], predicate: Predicate) {
        super(batches);
        this._predicate = predicate;
    }
    public scan(next: NextFunc, bind?: BindFunc) {
        // inlined version of this:
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) next(idx, columns);
        // });
        const batches = this._chunks;
        const numBatches = batches.length;
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            // TODO: bind batches lazily
            // If predicate doesn't match anything in the batch we don't need
            // to bind the callback
            if (bind) { bind(batch); }
            const predicate = this._predicate.bind(batch);
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                if (predicate(index, batch)) { next(index, batch); }
            }
        }
    }
    public count(): number {
        // inlined version of this:
        // let sum = 0;
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) ++sum;
        // });
        // return sum;
        let sum = 0;
        const batches = this._chunks;
        const numBatches = batches.length;
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            const predicate = this._predicate.bind(batch);
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                if (predicate(index, batch)) { ++sum; }
            }
        }
        return sum;
    }
    public *[Symbol.iterator](): IterableIterator<Struct<T>['TValue']> {
        // inlined version of this:
        // this.parent.scan((idx, columns) => {
        //     if (this.predicate(idx, columns)) next(idx, columns);
        // });
        const batches = this._chunks;
        const numBatches = batches.length;
        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            // TODO: bind batches lazily
            // If predicate doesn't match anything in the batch we don't need
            // to bind the callback
            const predicate = this._predicate.bind(batch);
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                if (predicate(index, batch)) { yield batch.get(index) as any; }
            }
        }
    }
    public filter(predicate: Predicate): FilteredDataFrame<T> {
        return new FilteredDataFrame<T>(
            this._chunks,
            this._predicate.and(predicate)
        );
    }
    public countBy(name: Col | string) {
        const batches = this._chunks, numBatches = batches.length;
        const count_by = typeof name === 'string' ? new Col(name) : name as Col;
        // Assume that all dictionary batches are deltas, which means that the
        // last record batch has the most complete dictionary
        count_by.bind(batches[numBatches - 1]);
        const vector = count_by.vector as V<Dictionary>;
        if (!DataType.isDictionary(vector.type)) {
            throw new Error('countBy currently only supports dictionary-encoded columns');
        }

        const countByteLength = Math.ceil(Math.log(vector.dictionary.length) / Math.log(256));
        const CountsArrayType = countByteLength == 4 ? Uint32Array :
                                countByteLength >= 2 ? Uint16Array : Uint8Array;

        const counts = new CountsArrayType(vector.dictionary.length);

        for (let batchIndex = -1; ++batchIndex < numBatches;) {
            // load batches
            const batch = batches[batchIndex];
            const predicate = this._predicate.bind(batch);
            // rebind the countBy Col
            count_by.bind(batch);
            const keys = (count_by.vector as V<Dictionary>).indices;
            // yield all indices
            for (let index = -1, numRows = batch.length; ++index < numRows;) {
                let key = keys.get(index);
                if (key !== null && predicate(index, batch)) { counts[key]++; }
            }
        }
        return new CountByResult(vector.dictionary, IntVector.from(counts));
    }
}
