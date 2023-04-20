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
        public class Builder : IArrowArrayBuilder<StructArray, Builder>
        {
            public readonly StructType DataType;
            IArrowType IArrowArrayBuilder.DataType => DataType;
            public List<IArrowArray> Arrays;
            private readonly ArrowBuffer.BitmapBuilder ValidityBufferBuilder;

            private List<IArrowArray> _arrays;

            public int Length { get; private set; }
            public int NullCount { get; private set; }

            public Builder(StructType structType)
            {
                DataType = structType;
                Arrays = null;
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder();

                // statics
                Length = 0;
                NullCount = 0;

                ResetCommit();
            }

            public IReadOnlyList<Field> Fields => DataType.Fields;

            private void ResetCommit()
            {
                _arrays = new List<IArrowArray>(Fields.Count);
            }

            private void Commit(MemoryAllocator allocator = default)
            {
                if (_arrays.Count != 0)
                {
                    int[] numRows = _arrays.Select(array => array.Length).Distinct().ToArray();

                    if (numRows.Length > 1)
                        throw new InvalidDataException($"All value builders do not have the same Length");

                    ValidityBufferBuilder.AppendRange(Enumerable.Repeat(true, numRows[0]));

                    // Set arrays
                    if (Arrays == null)
                    {
                        // First write
                        Arrays = _arrays;
                    }
                    else
                    {
                        // Merge with existing arrays
                        for (int i = 0; i < Fields.Count; i++)
                        {
                            Arrays[i] = ArrowArrayConcatenator.Concatenate(new IArrowArray[] { Arrays[i], _arrays[i] }, allocator);
                        }
                    }

                    Length = Arrays[0].Length;

                    ResetCommit();
                }
            }

            public Builder AppendArray(IArrowArray array, MemoryAllocator allocator = default)
            {
                // Get current buffer index
                int index = _arrays.Count;
                Field field = Fields[index];

                // Check Data consistency
                if (field.DataType.TypeId != array.Data.DataType.TypeId)
                    throw new InvalidDataException("Datatypes are differents");

                // Add array to commit
                _arrays.Add(array);

                // Check if need append, all column have been recieved, we can commit
                if (_arrays.Count == Fields.Count)
                    Commit(allocator);

                return this;
            }

            public Builder AppendArrays(IEnumerable<IArrowArray> arrays, MemoryAllocator allocator = default)
            {
                foreach (IArrowArray array in arrays)
                {
                    AppendArray(array, allocator);
                }

                return this;
            }

            public Builder AppendNull()
            {
                NullCount++;
                AppendArrays(Fields.Select(field => ArrowArrayFactory.BuildArrayWithNull(field.DataType)));
                ValidityBufferBuilder.Set(Length - 1, false);
                return this;
            }

            // Finalize
            public StructArray Build(MemoryAllocator allocator = default)
            {
                ArrowBuffer validityBuffer = NullCount > 0 ? ValidityBufferBuilder.Build(allocator) : ArrowBuffer.Empty;
                return new StructArray(DataType, Length, Arrays, validityBuffer, NullCount);
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
                ValidityBufferBuilder.Clear();
                return this;
            }
        }

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
