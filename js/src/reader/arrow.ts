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

import { flatbuffers } from 'flatbuffers';
import * as Schema_ from '../format/Schema_generated';
import * as Message_ from '../format/Message_generated';
export import Schema = Schema_.org.apache.arrow.flatbuf.Schema;
export import RecordBatch = Message_.org.apache.arrow.flatbuf.RecordBatch;

import { readFile } from './file';
import { readStream } from './stream';
import { readVector } from './vector';
import { readDictionary } from './dictionary';
import { Vector, Column } from '../types/types';

import ByteBuffer = flatbuffers.ByteBuffer;
import Field = Schema_.org.apache.arrow.flatbuf.Field;
export type Dictionaries = { [k: string]: Vector<any> } | null;
export type IteratorState = { nodeIndex: number; bufferIndex: number };

export function* readRecords(...bytes: ByteBuffer[]) {
    try {
        yield* readFile(...bytes);
    } catch (e) {
        try {
            yield* readStream(...bytes);
        } catch (e) {
            throw new Error('Invalid Arrow buffer');
        }
    }
}

export function* readBuffers(...bytes: Array<Uint8Array | Buffer | string>) {
    const dictionaries: Dictionaries = {};
    const byteBuffers = bytes.map(toByteBuffer);
    for (let { schema, batch } of readRecords(...byteBuffers)) {
        let vectors: Column<any>[] = [];
        let state = { nodeIndex: 0, bufferIndex: 0 };
        let fieldsLength = schema.fieldsLength();
        let index = -1, field: Field, vector: Vector<any>;
        if (batch.id) {
            // A dictionary batch only contain a single vector. Traverse each
            // field and its children until we find one that uses this dictionary
            while (++index < fieldsLength) {
                if (field = schema.fields(index)!) {
                    if (vector = readDictionary<any>(field, batch, state, dictionaries)!) {
                        dictionaries[batch.id] = dictionaries[batch.id] && dictionaries[batch.id].concat(vector) || vector;
                        break;
                    }
                }
            }
        } else {
            while (++index < fieldsLength) {
                if ((field = schema.fields(index)!) &&
                    (vector = readVector<any>(field, batch, state, dictionaries)!)) {
                    vectors[index] = vector as Column<any>;
                }
            }
            yield vectors;
        }
    }
}

function toByteBuffer(bytes?: Uint8Array | Buffer | string) {
    let arr: Uint8Array = bytes as any || new Uint8Array(0);
    if (typeof bytes === 'string') {
        arr = new Uint8Array(bytes.length);
        for (let i = -1, n = bytes.length; ++i < n;) {
            arr[i] = bytes.charCodeAt(i);
        }
        return new ByteBuffer(arr);
    }
    return new ByteBuffer(arr);
}
