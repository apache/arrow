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

import { Column } from '../column';
import { Vector } from '../vector';
import { Chunked } from '../vector/chunked';

type RecordBatchCtor = typeof import('../recordbatch').RecordBatch;

const isArray = Array.isArray;

/** @ignore */
export const selectAndFlatten = <T>(Ctor: any, vals: any[]) => _selectAndFlatten(Ctor, vals, [], 0) as T[];
/** @ignore */
export const selectAndFlattenChunks = <T>(Ctor: any, vals: any[]) => _selectAndFlattenChunks(Ctor, vals, [], 0) as T[];
/** @ignore */
export const selectAndFlattenVectorChildren = <T extends Vector>(Ctor: RecordBatchCtor, vals: any[]) => _selectAndFlattenVectorChildren(Ctor, vals, [], 0) as T[];
/** @ignore */
export const selectAndFlattenColumnChildren = <T extends Column>(Ctor: RecordBatchCtor, vals: any[]) => _selectAndFlattenColumnChildren(Ctor, vals, [], 0) as T[];

/** @ignore */
function _selectAndFlatten<T>(Ctor: any, vals: any[], ret: T[], idx: number) {
    for (let value: any, j = idx, i = -1, n = vals.length; ++i < n;) {
        if (isArray(value = vals[i])) {
            j = _selectAndFlatten(Ctor, value, ret, j).length;
        } else if (value instanceof Ctor) { ret[j++] = value; }
    }
    return ret;
}

/** @ignore */
function _selectAndFlattenChunks<T>(Ctor: any, vals: any[], ret: T[], idx: number) {
    for (let value: any, j = idx, i = -1, n = vals.length; ++i < n;) {
        if ((value = vals[i]) instanceof Chunked) {
            j = _selectAndFlattenChunks(Ctor, value.chunks, ret, j).length;
        } else if (value instanceof Ctor) { ret[j++] = value; }
    }
    return ret;
}

/** @ignore */
function _selectAndFlattenVectorChildren<T extends Vector>(Ctor: RecordBatchCtor, vals: any[], ret: T[], idx: number) {
    for (let value: any, j = idx, i = -1, n = vals.length; ++i < n;) {
        if (isArray(value = vals[i])) {
            j = _selectAndFlattenVectorChildren(Ctor, value, ret, j).length;
        } else if (value instanceof Ctor) {
            j = _selectAndFlatten(Vector, value.schema.fields.map((_, i) => value.getChildAt(i)!), ret, j).length;
        } else if (value instanceof Vector) { ret[j++] = value as T; }
    }
    return ret;
}

/** @ignore */
function _selectAndFlattenColumnChildren<T extends Column>(Ctor: RecordBatchCtor, vals: any[], ret: T[], idx: number) {
    for (let value: any, j = idx, i = -1, n = vals.length; ++i < n;) {
        if (isArray(value = vals[i])) {
            j = _selectAndFlattenColumnChildren(Ctor, value, ret, j).length;
        } else if (value instanceof Ctor) {
            j = _selectAndFlatten(Column, value.schema.fields.map((f, i) => Column.new(f, value.getChildAt(i)!)), ret, j).length;
        } else if (value instanceof Column) { ret[j++] = value as T; }
    }
    return ret;
}
