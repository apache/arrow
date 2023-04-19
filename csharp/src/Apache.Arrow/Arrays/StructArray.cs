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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class StructArray : Array
    {
#if NETCOREAPP3_1_OR_GREATER
        public class Builder : IArrowArrayBuilder<StructArray, Builder>
        {
            public readonly StructType DataType;
            IArrowType IArrowArrayBuilder.DataType => DataType;
            public readonly IArrowArrayBuilder[] Builders;
            private readonly ArrowBuffer.BitmapBuilder ValidityBufferBuilder;

            public int Length { get; private set; }
            public int NullCount { get; private set; }
            private int _commitSize;
            private bool[] _commitChecks;

            public Builder(StructType structType)
            {
                DataType = structType;
                Builders = structType.Fields
                    .Select(field => ArrowArrayBuilderFactory.Build(field.DataType))
                    .ToArray();
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder();

                // statics
                Length = 0;
                NullCount = 0;

                Append();
            }

            public IReadOnlyList<Field> Fields => DataType.Fields;

            public Builder Append()
            {
                _commitChecks = DataType.Fields.Select(_ => false).ToArray();
                return this;
            }

            public Builder FinalizeLastAppend(int numRows)
            {
                // Checks
                List<int> unfilled = new List<int>();
                for (int i = 0; i < _commitChecks.Length; i++)
                {
                    if (!_commitChecks[i])
                    {
                        unfilled.Add(i);
                    }
                }

                if (unfilled.Count > 0)
                    throw new InvalidDataException($"Fields ['{string.Join("','", unfilled.Select(i => DataType.Fields[i].Name))}'] were not filled in last append");

                if (Builders.Select(b => b.Length).Distinct().Count() > 1)
                    throw new InvalidDataException($"All value builders do not have the same Length");

                Length += numRows;

                Append();

                return this;
            }

            // Append in column
            public Builder AppendColumn(int index, IEnumerable<string> values)
            {
                IArrowArrayBuilder builder = Builders[index];

                switch (builder.DataType.TypeId)
                {
                    case ArrowTypeId.String:
                        (builder as StringArray.Builder).AppendRange(values);
                        break;
                    default:
                        throw new ArgumentException($"Cannot write string values in '{Fields[index]}' of type {builder.DataType.TypeId}");
                }

                _commitChecks[index] = true;

                return this;
            }

            public Builder AppendColumn(int index, IEnumerable<int?> values)
            {
                IArrowArrayBuilder builder = Builders[index];

                switch (builder.DataType.TypeId)
                {
                    case ArrowTypeId.Int32:
                        (builder as Int32Array.Builder).AppendRange(values);
                        break;
                    default:
                        throw new ArgumentException($"Cannot write string values in '{Fields[index]}' of type {builder.DataType.TypeId}");
                }

                _commitChecks[index] = true;

                return this;
            }

            public Builder AppendNull()
            {
                NullCount++;
                ValidityBufferBuilder.Append(false);
                return this;
            }

            // Finalize
            public StructArray Build(MemoryAllocator allocator = default)
            {
                ArrowBuffer validityBuffer = NullCount > 0 ? ValidityBufferBuilder.Build(allocator) : ArrowBuffer.Empty;
                IEnumerable<IArrowArray> arrays = Builders
                    .Select(builder => builder.GetType().GetMethod("Build").Invoke(builder, new object[] { allocator }) as IArrowArray);

                return new StructArray(DataType, Length, arrays, validityBuffer, NullCount);
            }
            public Builder Reserve(int capacity)
            {
                ValidityBufferBuilder.Reserve(capacity + 1);
                return this;
            }

            public Builder Resize(int length)
            {
                ValidityBufferBuilder.Resize(length + 1);
                return this;
            }

            public Builder Clear()
            {
                foreach (IArrowArrayBuilder builder in Builders)
                {
                    // invoke the method on the instance
                    builder.GetType().GetMethod("Clear").Invoke(builder, null);
                }
                ValidityBufferBuilder.Clear();
                return this;
            }
        }
#endif
        private IReadOnlyList<IArrowArray> _fields;

        public IReadOnlyList<IArrowArray> Fields =>
            LazyInitializer.EnsureInitialized(ref _fields, () => InitializeFields());

        public StructArray(
            IArrowType dataType, int length,
            IEnumerable<IArrowArray> children,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(
                dataType, length, nullCount, offset, new[] { nullBitmapBuffer },
                children.Select(child => child.Data)))
        {
            _fields = children.ToArray();
        }

        public StructArray(ArrayData data)
            : base(data)
        {
            data.EnsureDataType(ArrowTypeId.Struct);
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

        private IReadOnlyList<IArrowArray> InitializeFields()
        {
            IArrowArray[] result = new IArrowArray[Data.Children.Length];
            for (int i = 0; i < Data.Children.Length; i++)
            {
                result[i] = ArrowArrayFactory.BuildArray(Data.Children[i]);
            }
            return result;
        }

        public TArray GetArray<TArray>(int index) where TArray : Array => Fields[index] as TArray;
    }
}
