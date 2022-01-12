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

import { Table } from '../table.js';
import { TypeMap } from '../type.js';
import { isPromise } from '../util/compat.js';
import { FromArg0, FromArg1, FromArg2, FromArg3, FromArg4, FromArg5, RecordBatchReader } from './reader.js';
import { RecordBatchFileWriter, RecordBatchStreamWriter } from './writer.js';

/**
 * Deserialize the IPC format into a {@link Table}. This function is a
 * convenience wrapper for {@link RecordBatchReader}. Opposite of {@link tableToIPC}.
 */
export function tableFromIPC<T extends TypeMap = any>(source: FromArg0 | FromArg2): Table<T>;
export function tableFromIPC<T extends TypeMap = any>(source: FromArg1): Promise<Table<T>>;
export function tableFromIPC<T extends TypeMap = any>(source: FromArg3 | FromArg4 | FromArg5): Promise<Table<T>> | Table<T>;
export function tableFromIPC<T extends TypeMap = any>(input: any): Table<T> | Promise<Table<T>> {
    const reader = RecordBatchReader.from<T>(input);
    if (isPromise(reader)) {
        return (async () => new Table(await (await reader).readAll()))();
    }
    return new Table(reader.readAll());
}

/**
 * Serialize a {@link Table} to the IPC format. This function is a convenience
 * wrapper for {@link RecordBatchStreamWriter} and {@link RecordBatchFileWriter}.
 * Opposite of {@link tableFromIPC}.
 *
 * @param table The Table to serialize.
 * @param type Whether to serialize the Table as a file or a stream.
 */
export function tableToIPC<T extends TypeMap = any>(table: Table, type: 'file' | 'stream' = 'stream'): Uint8Array {
    return (type === 'stream' ? RecordBatchStreamWriter : RecordBatchFileWriter)
        .writeAll<T>(table)
        .toUint8Array(true);
}
