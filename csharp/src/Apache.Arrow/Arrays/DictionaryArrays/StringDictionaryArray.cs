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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow.Arrays
{
    public class StringDictionaryArray : DictionaryArray
    {
        public static readonly Encoding DefaultEncoding = Encoding.UTF8;

        public ArrowBuffer ValueBuffer => Data.Buffers[2];

        public ReadOnlySpan<int> ValueOffsets => ValueOffsetsBuffer.Span.CastTo<int>().Slice(0, UniqueValuesCount+1);
        
        public ArrowBuffer IndicesBuffer => Data.Buffers[1];

        public override ReadOnlySpan<int> Indices => IndicesBuffer.Span.CastTo<int>().Slice(0, Length);

        public ReadOnlySpan<byte> Values => ValueBuffer.Span.CastTo<byte>().Slice(0, ValueOffsets[UniqueValuesCount]);

        public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[3];

        /// <inheritdoc />
        public StringDictionaryArray(ArrayData data, int uniqueValuesCount) : base(data, uniqueValuesCount)
        {
            Data.EnsureBufferCount(4);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetValueOffset(int index)
        {
            index = Indices[index]; // get dictionary value index from entry index
            return ValueOffsets[Offset + index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetValueLength(int index)
        {
            // get dictionary value index from entry index
            index = Indices[index];
            var offsets = ValueOffsets;
            var offset = Offset + index;

            return offsets[offset + 1] - offsets[offset];
        }

        public ReadOnlySpan<byte> GetBytes(int index)
        {
            var offset = GetValueOffset(index);
            var length = GetValueLength(index);

            return ValueBuffer.Span.Slice(offset, length);
        }

        public string GetString(int index, Encoding encoding = default)
        {
            encoding = encoding ?? DefaultEncoding;

            var bytes = GetBytes(index);

            unsafe
            {
                fixed (byte* data = &MemoryMarshal.GetReference(bytes))
                    return encoding.GetString(data, bytes.Length);
            }
        }

        public StringDictionaryArray(int length,
            int uniqueValues,
            ArrowBuffer nullBitmapBuffer,
            ArrowBuffer indices,
            ArrowBuffer dataBuffer,
            ArrowBuffer dataOffsets,
            int nullCount = 0, int offset = 0)
            : this(new ArrayData(DictionaryType.Default(ArrowTypeId.String), length, nullCount, offset,
                new[] { nullBitmapBuffer, indices, dataBuffer, dataOffsets }), uniqueValues)
        { }

        public class StringDictionaryBuilder : DictionaryArrayBuilderBase<string>, IDictionaryArrayBuilder<string, StringDictionaryArray, StringDictionaryBuilder>
        {

            public Encoding Encoding;

            protected ArrowBuffer.Builder<int> ValueOffsets { get; }
            protected ArrowBuffer.Builder<byte> ValueBuffer { get; }
            protected int Offset { get; set; }

            protected int NullCount = 0;

            public StringDictionaryBuilder() : this(null, null, null){ }


            /// <inheritdoc />
            public StringDictionaryBuilder(Encoding encoding = null, IEqualityComparer<string> comparer = null, HashFunctionDelegate hashFunc = null) : base(comparer, hashFunc)
            {
                if (encoding == null)
                    encoding = DefaultEncoding;

                Encoding = encoding;
                ValueOffsets = new ArrowBuffer.Builder<int>();
                ValueBuffer = new ArrowBuffer.Builder<byte>();
            }


            public StringDictionaryArray Build(MemoryAllocator allocator)
            {
                ValueOffsets.Append(Offset);
                return new StringDictionaryArray(IndicesBuffer.Length, Entries.Count, NullBitmap.Build(allocator), IndicesBuffer.Build(allocator),
                    ValueBuffer.Build(allocator), ValueOffsets.Build(allocator), NullCount);
            }


            /// <inheritdoc />
            public StringDictionaryBuilder Append(string value)
            {
                if (value == null)
                {
                    NullBitmap.Append(0);
                    IndicesBuffer.Append(-1); // need something in the indices buffer to make sure it makes sense
                    NullCount++;
                    return this;
                }


                var temp = new DictionaryEntry(value, Comparer, HashFunction);
                if (!Entries.TryGetValue(temp, out var index))
                {
                    index = NextIndex++;
                    Entries.Add(temp, index);
                    AppendStringToBuffer(value);
                }

                NullBitmap.Append(1);
                IndicesBuffer.Append(index);
                return this;
            }

            private void AppendStringToBuffer(string s)
            {
                var span = Encoding.GetBytes(s);
                ValueOffsets.Append(Offset);
                ValueBuffer.Append(span);
                Offset += span.Length;
            }

            public StringDictionaryBuilder Append(ReadOnlySpan<string> span)
            {
                foreach (var s in span)
                {
                    Append(s);
                }

                return this;
            }

            public StringDictionaryBuilder AppendRange(IEnumerable<string> values)
            {
                foreach (var s in values)
                {
                    Append(s);
                }

                return this;
            }

            public StringDictionaryBuilder Reserve(int capacity)
            {
                IndicesBuffer.Reserve(capacity);
                return this;
            }

            public StringDictionaryBuilder Resize(int length)
            {
                IndicesBuffer.Reserve(length);
                return this;
            }

            public StringDictionaryBuilder Swap(int i, int j)
            {
                throw new NotImplementedException();
            }

            public StringDictionaryBuilder Set(int index, string value)
            {
                throw new NotImplementedException();
            }

            public StringDictionaryBuilder Clear()
            {
                Entries.Clear();
                IndicesBuffer.Clear();
                NullBitmap.Clear();
                NextIndex = 0;
                return this;
            }
        }
    }
}
