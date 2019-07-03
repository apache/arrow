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

using Apache.Arrow.Types;
using System;

namespace Apache.Arrow
{
    public static class ArrowArrayFactory
    {
        public static IArrowArray BuildArray(ArrayData data)
        {
            switch (data.DataType.TypeId)
            {
                case ArrowTypeId.Boolean:
                    return new BooleanArray(data);
                case ArrowTypeId.UInt8:
                    return new UInt8Array(data);
                case ArrowTypeId.Int8:
                    return new Int8Array(data);
                case ArrowTypeId.UInt16:
                    return new UInt16Array(data);
                case ArrowTypeId.Int16:
                    return new Int16Array(data);
                case ArrowTypeId.UInt32:
                    return new UInt32Array(data);
                case ArrowTypeId.Int32:
                    return new Int32Array(data);
                case ArrowTypeId.UInt64:
                    return new UInt64Array(data);
                case ArrowTypeId.Int64:
                    return new Int64Array(data);
                case ArrowTypeId.Float:
                    return new FloatArray(data);
                case ArrowTypeId.Double:
                    return new DoubleArray(data);
                case ArrowTypeId.String:
                    return new StringArray(data);
                case ArrowTypeId.Binary:
                    return new BinaryArray(data);
                case ArrowTypeId.Timestamp:
                    return new TimestampArray(data);
                case ArrowTypeId.List:
                    return new ListArray(data);
                case ArrowTypeId.Struct:
                    return new StructArray(data);
                case ArrowTypeId.Union:
                    return new UnionArray(data);
                case ArrowTypeId.Date64:
                case ArrowTypeId.Date32:
                case ArrowTypeId.Decimal:
                case ArrowTypeId.Dictionary:
                case ArrowTypeId.FixedSizedBinary:
                case ArrowTypeId.HalfFloat:
                case ArrowTypeId.Interval:
                case ArrowTypeId.Map:
                case ArrowTypeId.Time32:
                case ArrowTypeId.Time64:
                default:
                    throw new NotSupportedException($"An ArrowArray cannot be built for type {data.DataType.TypeId}.");
            }
        }
    }
}
