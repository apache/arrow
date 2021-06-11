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
using System.IO;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class DictionaryArray : Array
    {
        public IArrowArray Dictionary { get; }
        public IArrowArray Indices { get; }
        public ArrowBuffer IndicesBuffer => Data.Buffers[1];

        public DictionaryArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, IArrowArray value,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(new ArrayData(dataType, length, nullCount, offset,
                new[] { nullBitmapBuffer, valueOffsetsBuffer }, new[] { value.Data }, value.Data.Dictionary))
        {
        }

        public DictionaryArray(ArrayData data) : base(data)
        {
            data.EnsureBufferCount(2);
            data.EnsureDataType(ArrowTypeId.Dictionary);

            var dicType = data.DataType as DictionaryType;
            data.Dictionary.EnsureDataType(dicType.ValueType.TypeId);

            ArrayData indicesData = new ArrayData(dicType.IndexType, data.Length, data.NullCount, data.Offset, data.Buffers, data.Children);

            Indices = ArrowArrayFactory.BuildArray(indicesData);
            Dictionary = ArrowArrayFactory.BuildArray(data.Dictionary);
        }

        public DictionaryArray(IArrowType dataType, IArrowArray indicesArray, IArrowArray dictionary, bool ordered = false) :
            base(new ArrayData(dataType, indicesArray.Length, indicesArray.Data.NullCount, indicesArray.Data.Offset, indicesArray.Data.Buffers, indicesArray.Data.Children, dictionary.Data))
        {
            Data.EnsureBufferCount(2);
            Data.EnsureDataType(ArrowTypeId.Dictionary);

            var dicType = dataType as DictionaryType;

            indicesArray.Data.EnsureDataType(dicType.IndexType.TypeId);
            dictionary.Data.EnsureDataType(dicType.ValueType.TypeId);

            Indices = indicesArray;
            Dictionary = dictionary;
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);
    }
}
