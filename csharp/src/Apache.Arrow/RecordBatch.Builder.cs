// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow
{
    public partial class RecordBatch
    {
        public class ArrayBuilder
        {
            private readonly MemoryAllocator _allocator;

            internal ArrayBuilder(MemoryAllocator allocator)
            {
                _allocator = allocator;
            }

            public BooleanArray Boolean(Action<BooleanArray.Builder> action) => Build<BooleanArray, BooleanArray.Builder>(action);
            public Int8Array Int8(Action<Int8Array.Builder> action) => Build<Int8Array, Int8Array.Builder>(action);
            public Int16Array Int16(Action<Int16Array.Builder> action) => Build<Int16Array, Int16Array.Builder>(action);
            public Int32Array Int32(Action<Int32Array.Builder> action) => Build<Int32Array, Int32Array.Builder>(action);
            public Int64Array Int64(Action<Int64Array.Builder> action) => Build<Int64Array, Int64Array.Builder>(action);
            public UInt8Array UInt8(Action<UInt8Array.Builder> action) => Build<UInt8Array, UInt8Array.Builder>(action);
            public UInt16Array UInt16(Action<UInt16Array.Builder> action) => Build<UInt16Array, UInt16Array.Builder>(action);
            public UInt32Array UInt32(Action<UInt32Array.Builder> action) => Build<UInt32Array, UInt32Array.Builder>(action);
            public UInt64Array UInt64(Action<UInt64Array.Builder> action) => Build<UInt64Array, UInt64Array.Builder>(action);
            public FloatArray Float(Action<FloatArray.Builder> action) => Build<FloatArray, FloatArray.Builder>(action);
            public DoubleArray Double(Action<DoubleArray.Builder> action) => Build<DoubleArray, DoubleArray.Builder>(action);
            public Date32Array Date32(Action<Date32Array.Builder> action) => Build<Date32Array, Date32Array.Builder>(action);
            public Date64Array Date64(Action<Date64Array.Builder> action) => Build<Date64Array, Date64Array.Builder>(action);
            public BinaryArray Binary(Action<BinaryArray.Builder> action) => Build<BinaryArray, BinaryArray.Builder>(action);
            public StringArray String(Action<StringArray.Builder> action) => Build<StringArray, StringArray.Builder>(action);

            private TArray Build<TArray, TArrayBuilder>(Action<TArrayBuilder> action)
                where TArray: IArrowArray
                where TArrayBuilder: IArrowArrayBuilder<TArray>, new()
            {
                if (action == null)
                {
                    return default;
                }

                var builder = new TArrayBuilder();
                action(builder);

                return builder.Build(_allocator);
            }
        }

        public class Builder
        {
            private readonly MemoryAllocator _allocator;
            private readonly ArrayBuilder _arrayBuilder;
            private readonly Schema.Builder _schemaBuilder;
            private readonly List<IArrowArray> _arrays;

            public Builder(MemoryAllocator allocator = default)
            {
                _allocator = allocator ?? MemoryAllocator.Default.Value;
                _arrayBuilder = new ArrayBuilder(_allocator);
                _schemaBuilder = new Schema.Builder();
                _arrays = new List<IArrowArray>();
            }

            public RecordBatch Build()
            {
                var schema = _schemaBuilder.Build();
                var length = _arrays.Max(x => x.Length);

                // each array has its own memoryOwner, so the RecordBatch itself doesn't
                // have a memoryOwner
                IMemoryOwner<byte> memoryOwner = null;
                var batch = new RecordBatch(schema, memoryOwner, _arrays, length);

                return batch;
            }

            public Builder Clear()
            {
                _schemaBuilder.Clear();
                _arrays.Clear();
                return this;
            }

            public Builder Append(RecordBatch batch)
            {
                foreach (var field in batch.Schema.Fields)
                {
                    _schemaBuilder.Field(field.Value);
                }

                foreach (var array in batch.Arrays)
                {
                    _arrays.Add(array);
                }

                return this;
            }

            public Builder Append<TArray>(string name, bool nullable, IArrowArrayBuilder<TArray> builder)
                where TArray: IArrowArray
            {
                return builder == null 
                    ? this 
                    : Append(name, nullable, builder.Build(_allocator));
            }

            public Builder Append<TArray>(string name, bool nullable, TArray array)
                where TArray: IArrowArray
            {
                if (string.IsNullOrWhiteSpace(name)) throw new ArgumentNullException(nameof(name));
                if (array == null) return this;

                _arrays.Add(array);

                _schemaBuilder.Field(f => f
                    .Name(name)
                    .Nullable(nullable)
                    .DataType(array.Data.DataType));

                return this;
            }

            public Builder Append<TArray>(string name, bool nullable, Func<ArrayBuilder, TArray> action)
                where TArray: IArrowArray
            {
                if (action == null) return this;

                var array = action(_arrayBuilder);

                Append(name, nullable, array);

                return this;
            }

        }
    }
}