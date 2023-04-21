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
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    public class MapArray : ListArray
    {
        public IArrowArray Keys { get; }

        public MapArray(IArrowType dataType, int length,
            ArrowBuffer valueOffsetsBuffer, IArrowArray keys, IArrowArray values,
            ArrowBuffer nullBitmapBuffer, int nullCount = 0, int offset = 0)
            : this(
                  new ArrayData(
                      dataType, length, nullCount, offset, new[] { nullBitmapBuffer, valueOffsetsBuffer },
                      new[] { keys.Data, values.Data }
                  ), keys, values)
        {
        }

        public MapArray(ArrayData data)
            : this(data, ArrowArrayFactory.BuildArray(data.Children[0]), ArrowArrayFactory.BuildArray(data.Children[1]))
        {
        }

        private MapArray(ArrayData data, IArrowArray keys, IArrowArray values) : base(data, values, ArrowTypeId.Map)
        {
            Keys = keys;
        }

        public Tuple<TKeyArray, TValueArray> GetKeyValueArray<TKeyArray, TValueArray>(int index)
            where TKeyArray : Array where TValueArray : Array
        {
            if (Keys is not Array keys || Values is not Array values)
            {
                return default;
            }

            ReadOnlySpan<int> offsets = ValueOffsets;
            return Tuple.Create(
                keys.Slice(offsets[index], offsets[index + 1] - offsets[index]) as TKeyArray,
                values.Slice(offsets[index], offsets[index + 1] - offsets[index]) as TValueArray
            );
        }
    }
}
