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

        public DictionaryArray(ArrayData data) : base(data)
        {
            data.EnsureBufferCount(2);
            data.EnsureDataType(ArrowTypeId.Dictionary);

            if (data.Dictionary == null)
            {
                throw new ArgumentException($"{nameof(data.Dictionary)} must not be null");
            }

            var dicType = (DictionaryType)data.DataType;
            data.Dictionary.EnsureDataType(dicType.ValueType.TypeId);

            var indicesData = new ArrayData(dicType.IndexType, data.Length, data.NullCount, data.Offset, data.Buffers, data.Children);

            Indices = ArrowArrayFactory.BuildArray(indicesData);
            Dictionary = ArrowArrayFactory.BuildArray(data.Dictionary);
        }

        public DictionaryArray(DictionaryType dataType, IArrowArray indicesArray, IArrowArray dictionary) :
            base(new ArrayData(dataType, indicesArray.Length, indicesArray.Data.NullCount, indicesArray.Data.Offset, indicesArray.Data.Buffers, indicesArray.Data.Children, dictionary.Data))
        {
            Data.EnsureBufferCount(2);

            indicesArray.Data.EnsureDataType(dataType.IndexType.TypeId);
            dictionary.Data.EnsureDataType(dataType.ValueType.TypeId);

            Indices = indicesArray;
            Dictionary = dictionary;
        }

        public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);
    }
}
