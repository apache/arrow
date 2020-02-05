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

import { Data } from '../data';
import { Field } from '../schema';
import { Column } from '../column';
import { Vector } from '../vector';
import { DataType } from '../type';
import { Chunked } from '../vector/chunked';

type RecordBatchCtor = typeof import('../recordbatch').RecordBatch;

const isArray = Array.isArray;

/** @ignore */
export const selectArgs = <T>(Ctor: any, vals: any[]) => _selectArgs(Ctor, vals, [], 0) as T[];
/** @ignore */
export const selectColumnArgs = <T extends { [key: string]: DataType }>(args: any[]) => {
    const [fields, values] = _selectFieldArgs<T>(args, [[], []]);
    return values.map((x, i) =>
        x instanceof Column ? Column.new(x.field.clone(fields[i]), x) :
        x instanceof Vector ? Column.new(fields[i], x) as Column<T[keyof T]> :
                              Column.new(fields[i], [] as Vector<T[keyof T]>[]));
};

/** @ignore */
export const selectFieldArgs = <T extends { [key: string]: DataType }>(args: any[]) => _selectFieldArgs<T>(args, [[], []]);
/** @ignore */
export const selectChunkArgs = <T>(Ctor: any, vals: any[]) => _selectChunkArgs(Ctor, vals, [], 0) as T[];
/** @ignore */
export const selectVectorChildrenArgs = <T extends Vector>(Ctor: RecordBatchCtor, vals: any[]) => _selectVectorChildrenArgs(Ctor, vals, [], 0) as T[];
/** @ignore */
export const selectColumnChildrenArgs = <T extends Column>(Ctor: RecordBatchCtor, vals: any[]) => _selectColumnChildrenArgs(Ctor, vals, [], 0) as T[];

/** @ignore */
function _selectArgs<T>(Ctor: any, vals: any[], res: T[], idx: number) {
    let value: any, j = idx;
    let i = -1, n = vals.length;
    while (++i < n) {
        if (isArray(value = vals[i])) {
            j = _selectArgs(Ctor, value, res, j).length;
        } else if (value instanceof Ctor) { res[j++] = value; }
    }
    return res;
}

/** @ignore */
function _selectChunkArgs<T>(Ctor: any, vals: any[], res: T[], idx: number) {
    let value: any, j = idx;
    let i = -1, n = vals.length;
    while (++i < n) {
        if (isArray(value = vals[i])) {
            j = _selectChunkArgs(Ctor, value, res, j).length;
        } else if (value instanceof Chunked) {
            j = _selectChunkArgs(Ctor, value.chunks, res, j).length;
        } else if (value instanceof Ctor) { res[j++] = value; }
    }
    return res;
}

/** @ignore */
function _selectVectorChildrenArgs<T extends Vector>(Ctor: RecordBatchCtor, vals: any[], res: T[], idx: number) {
    let value: any, j = idx;
    let i = -1, n = vals.length;
    while (++i < n) {
        if (isArray(value = vals[i])) {
            j = _selectVectorChildrenArgs(Ctor, value, res, j).length;
        } else if (value instanceof Ctor) {
            j = _selectArgs(Vector, value.schema.fields.map((_, i) => value.getChildAt(i)!), res, j).length;
        } else if (value instanceof Vector) { res[j++] = value as T; }
    }
    return res;
}

/** @ignore */
function _selectColumnChildrenArgs<T extends Column>(Ctor: RecordBatchCtor, vals: any[], res: T[], idx: number) {
    let value: any, j = idx;
    let i = -1, n = vals.length;
    while (++i < n) {
        if (isArray(value = vals[i])) {
            j = _selectColumnChildrenArgs(Ctor, value, res, j).length;
        } else if (value instanceof Ctor) {
            j = _selectArgs(Column, value.schema.fields.map((f, i) => Column.new(f, value.getChildAt(i)!)), res, j).length;
        } else if (value instanceof Column) { res[j++] = value as T; }
    }
    return res;
}

/** @ignore */
const toKeysAndValues = (xs: [any[], any[]], [k, v]: [any, any], i: number) => (xs[0][i] = k, xs[1][i] = v, xs);

/** @ignore */
function _selectFieldArgs<T extends { [key: string]: DataType }>(vals: any[], ret: [Field<T[keyof T]>[], Vector<T[keyof T]>[]]): [Field<T[keyof T]>[], (T[keyof T] | Vector<T[keyof T]>)[]] {
    let keys: any[], n: number;
    switch (n = vals.length) {
        case 0: return ret;
        case 1:
            keys = ret[0];
            if (!(vals[0])) { return ret; }
            if (isArray(vals[0])) { return _selectFieldArgs(vals[0], ret); }
            if (!(vals[0] instanceof Data || vals[0] instanceof Vector || vals[0] instanceof DataType)) {
                [keys, vals] = Object.entries(vals[0]).reduce(toKeysAndValues, ret);
            }
            break;
        default:
            !isArray(keys = vals[n - 1])
                ? (vals = isArray(vals[0]) ? vals[0] : vals, keys = [])
                : (vals = isArray(vals[0]) ? vals[0] : vals.slice(0, n - 1));
    }

    let fieldIndex = -1;
    let valueIndex = -1;
    let idx = -1, len = vals.length;
    let field: number | string | Field<T[keyof T]>;
    let val: Vector<T[keyof T]> | Data<T[keyof T]>;
    let [fields, values] = ret as [Field<T[keyof T]>[], any[]];

    while (++idx < len) {
        val = vals[idx];
        if (val instanceof Column && (values[++valueIndex] = val)) {
            fields[++fieldIndex] = val.field.clone(keys[idx], val.type, true);
        } else {
            ({ [idx]: field = idx } = keys);
            if (val instanceof DataType && (values[++valueIndex] = val)) {
                fields[++fieldIndex] = Field.new(field, val as DataType, true) as Field<T[keyof T]>;
            } else if (val && val.type && (values[++valueIndex] = val)) {
                val instanceof Data && (values[valueIndex] = val = Vector.new(val) as Vector);
                fields[++fieldIndex] = Field.new(field, val.type, true) as Field<T[keyof T]>;
            }
        }
    }
    return ret;
}
