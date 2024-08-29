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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class MapArray : ListArray // MapArray = ListArray(StructArray("key", "value")) 
    {
        // Same as ListArray.Builder, but with KeyBuilder
        public new class Builder : IArrowArrayBuilder<MapArray, Builder>
        {
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> KeyBuilder { get; }
            public IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> ValueBuilder { get; }

            public int Length => ValueOffsetsBufferBuilder.Length;

            private ArrowBuffer.Builder<int> ValueOffsetsBufferBuilder { get; }

            private ArrowBuffer.BitmapBuilder ValidityBufferBuilder { get; }

            public int NullCount { get; protected set; }

            public MapType DataType { get; }

            public Builder(MapType type)
            {
                KeyBuilder = ArrowArrayBuilderFactory.Build(type.KeyField.DataType);
                ValueBuilder = ArrowArrayBuilderFactory.Build(type.ValueField.DataType);
                ValueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
                ValidityBufferBuilder = new ArrowBuffer.BitmapBuilder();
                DataType = type;
            }

            /// <summary>
            /// Start a new variable-length list slot
            ///
            /// This function should be called before beginning to append elements to the
            /// value builder
            /// </summary>
            /// <returns></returns>
            public Builder Append()
            {
                ValueOffsetsBufferBuilder.Append(KeyBuilder.Length);
                ValidityBufferBuilder.Append(true);

                return this;
            }

            public Builder AppendNull()
            {
                ValueOffsetsBufferBuilder.Append(KeyBuilder.Length);
                ValidityBufferBuilder.Append(false);
                NullCount++;

                return this;
            }

            public MapArray Build(MemoryAllocator allocator = default)
            {
                ValueOffsetsBufferBuilder.Append(KeyBuilder.Length);

                ArrowBuffer validityBuffer = NullCount > 0 ? ValidityBufferBuilder.Build(allocator) : ArrowBuffer.Empty;

                StructArray structs = new StructArray(
                    DataType.KeyValueType, KeyBuilder.Length,
                    new IArrowArray[] { KeyBuilder.Build(allocator), ValueBuilder.Build(allocator) },
                    ArrowBuffer.Empty, 0
                );

                return new MapArray(DataType, Length - 1, ValueOffsetsBufferBuilder.Build(allocator), structs, validityBuffer, NullCount);
            }

            public Builder Reserve(int capacity)
            {
                ValueOffsetsBufferBuilder.Reserve(capacity + 1);
                ValidityBufferBuilder.Reserve(capacity);
                return this;
            }

            public Builder Resize(int length)
            {
                ValueOffsetsBufferBuilder.Resize(length + 1);
                ValidityBufferBuilder.Resize(length);
                return this;
            }

            public Builder Clear()
            {
                ValueOffsetsBufferBuilder.Clear();
                KeyBuilder.Clear();
                ValueBuilder.Clear();
                ValidityBufferBuilder.Clear();
                return this;
            }

        }

        public StructArray KeyValues => base.Values as StructArray;
        public IArrowArray Keys => KeyValues.Fields[0];
        public new IArrowArray Values => KeyValues.Fields[1];

        public MapArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, IArrowArray structs,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(
                  new ArrayData(
                      dataType, length, nullCount, offset, new[] { nullBitmapBuffer, valueOffsetsBuffer },
                      new[] { structs.Data }
                  ), structs)
        {
        }

        public MapArray(ArrayData data)
            : this(data, ArrowArrayFactory.BuildArray(data.Children[0]))
        {
        }

        private MapArray(ArrayData data, IArrowArray structs) : base(data, structs, ArrowTypeId.Map)
        {
        }

        public IEnumerable<Tuple<K, V>> GetTuples<TKeyArray, K, TValueArray, V>(int index, Func<TKeyArray, int, K> getKey, Func<TValueArray, int, V> getValue)
            where TKeyArray : Array where TValueArray : Array
        {
            ReadOnlySpan<int> offsets = ValueOffsets;
            // Get key values
            int start = offsets[index];
            int end = offsets[index + 1];
            StructArray array = KeyValues.Slice(start, end - start) as StructArray;

            TKeyArray keyArray = array.Fields[0] as TKeyArray;
            TValueArray valueArray = array.Fields[1] as TValueArray;

            for (int i = start; i < end; i++)
            {
                yield return new Tuple<K, V>(getKey(keyArray, i), getValue(valueArray, i));
            }
        }

        public IEnumerable<KeyValuePair<K,V>> GetKeyValuePairs<TKeyArray, K, TValueArray, V>(int index, Func<TKeyArray, int, K> getKey, Func<TValueArray, int, V> getValue)
            where TKeyArray : Array where TValueArray : Array
        {
            ReadOnlySpan<int> offsets = ValueOffsets;
            // Get key values
            int start = offsets[index];
            int end = offsets[index + 1];
            StructArray array = KeyValues.Slice(start, end - start) as StructArray;

            TKeyArray keyArray = array.Fields[0] as TKeyArray;
            TValueArray valueArray = array.Fields[1] as TValueArray;

            for (int i = start; i < end; i++)
            {
                yield return new KeyValuePair<K,V>(getKey(keyArray, i), getValue(valueArray, i));
            }
        }
    }
}
