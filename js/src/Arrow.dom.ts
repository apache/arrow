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
import { RecordBatchReader } from './ipc/reader';
import { RecordBatchWriter } from './ipc/writer';
import { toDOMStream } from './ipc/whatwg/iterable';
import { recordBatchReaderThroughDOMStream } from './ipc/whatwg/reader';
import { recordBatchWriterThroughDOMStream } from './ipc/whatwg/writer';

streamAdapters.toDOMStream = toDOMStream;
RecordBatchReader['throughDOM'] = recordBatchReaderThroughDOMStream;
RecordBatchWriter['throughDOM'] = recordBatchWriterThroughDOMStream;

export {
    ArrowType, DateUnit, IntervalUnit, MessageHeader, MetadataVersion, Precision, TimeUnit, Type, UnionMode, VectorType,
    Data,
    DataType,
    Null,
    Bool,
    Int, Int8, Int16, Int32, Int64, Uint8, Uint16, Uint32, Uint64,
    Float, Float16, Float32, Float64,
    Utf8,
    Binary,
    FixedSizeBinary,
    Date_, DateDay, DateMillisecond,
    Timestamp, TimestampSecond, TimestampMillisecond, TimestampMicrosecond, TimestampNanosecond,
    Time, TimeSecond, TimeMillisecond, TimeMicrosecond, TimeNanosecond,
    Decimal,
    List,
    Struct,
    Union, DenseUnion, SparseUnion,
    Dictionary,
    Interval, IntervalDayTime, IntervalYearMonth,
    FixedSizeList,
    Map_,
    Table,
    Column,
    Schema, Field,
    Visitor,
    Vector,
    BaseVector,
    BinaryVector,
    BoolVector,
    Chunked,
    DateVector, DateDayVector, DateMillisecondVector,
    DecimalVector,
    DictionaryVector,
    FixedSizeBinaryVector,
    FixedSizeListVector,
    FloatVector, Float16Vector, Float32Vector, Float64Vector,
    IntervalVector, IntervalDayTimeVector, IntervalYearMonthVector,
    IntVector, Int8Vector, Int16Vector, Int32Vector, Int64Vector, Uint8Vector, Uint16Vector, Uint32Vector, Uint64Vector,
    ListVector,
    MapVector,
    NullVector,
    StructVector,
    TimestampVector, TimestampSecondVector, TimestampMillisecondVector, TimestampMicrosecondVector, TimestampNanosecondVector,
    TimeVector, TimeSecondVector, TimeMillisecondVector, TimeMicrosecondVector, TimeNanosecondVector,
    UnionVector, DenseUnionVector, SparseUnionVector,
    Utf8Vector,
    ByteStream, AsyncByteStream, AsyncByteQueue, ReadableSource, WritableSink,
    RecordBatchReader, RecordBatchFileReader, RecordBatchStreamReader, AsyncRecordBatchFileReader, AsyncRecordBatchStreamReader,
    RecordBatchWriter, RecordBatchFileWriter, RecordBatchStreamWriter, RecordBatchJSONWriter,
    MessageReader, AsyncMessageReader, JSONMessageReader,
    Message,
    RecordBatch,
    ArrowJSONLike, FileHandle, Readable, Writable, ReadableWritable, ReadableDOMStreamOptions,
    DataFrame, FilteredDataFrame, CountByResult, BindFunc, NextFunc,
    predicate,
    util
} from './Arrow';
