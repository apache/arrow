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
import { RecordBatchStreamReader } from './reader.js';
import { RecordBatchStreamWriter } from './writer.js';

/**
 * Deserialize the IPC byte formast into a {@link Table}.
 * This function is a convenience wrapper for {@link RecordBatchStreamReader}.
 */
export function deserialize<T extends TypeMap = any>(bytes: Uint8Array): Table<T> {
    return new Table([
        ...RecordBatchStreamReader.from<T>(bytes)
    ]);
}

/**
 * Serialize a {@link Table} to the IPC format.
 * This function is a convenience wrapper for {@link RecordBatchStreamWriter}.
 */
export function serialize<T extends TypeMap = any>(table: Table): Uint8Array {
    return RecordBatchStreamWriter
        .writeAll<T>(table)
        .toUint8Array(true);
}
