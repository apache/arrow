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

import streamAdapters from './io/adapters';
import { Builder } from './builder/index';
import { RecordBatchReader } from './ipc/reader';
import { RecordBatchWriter } from './ipc/writer';
import { toNodeStream } from './io/node/iterable';
import { builderThroughNodeStream } from './io/node/builder';
import { recordBatchReaderThroughNodeStream } from './io/node/reader';
import { recordBatchWriterThroughNodeStream } from './io/node/writer';

streamAdapters.toNodeStream = toNodeStream;
Builder['throughNode'] = builderThroughNodeStream;
RecordBatchReader['throughNode'] = recordBatchReaderThroughNodeStream;
RecordBatchWriter['throughNode'] = recordBatchWriterThroughNodeStream;

export * from './Arrow.dom';

export * from './add/binary'
export * from './add/bool'
export * from './add/date'
export * from './add/decimal'
export * from './add/dictionary'
export * from './add/fixedSizeBinary'
export * from './add/fixedSizeList'
export * from './add/float'
export * from './add/interval'
export * from './add/int'
export * from './add/list'
export * from './add/map'
export * from './add/null'
export * from './add/struct'
export * from './add/timestamp'
export * from './add/time'
export * from './add/union'
export * from './add/utf8'
