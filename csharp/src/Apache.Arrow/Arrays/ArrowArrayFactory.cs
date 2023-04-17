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
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

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
                case ArrowTypeId.FixedSizedBinary:
                    return new FixedSizeBinaryArray(data);
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
                    return new Date64Array(data);
                case ArrowTypeId.Date32:
                    return new Date32Array(data);
                case ArrowTypeId.Time32:
                    return new Time32Array(data);
                case ArrowTypeId.Time64:
                    return new Time64Array(data);
                case ArrowTypeId.Decimal128:
                    return new Decimal128Array(data);
                case ArrowTypeId.Decimal256:
                    return new Decimal256Array(data);
                case ArrowTypeId.Dictionary:
                    return new DictionaryArray(data);
                case ArrowTypeId.HalfFloat:
#if NET5_0_OR_GREATER
                    return new HalfFloatArray(data);
#else
                    throw new NotSupportedException("Half-float arrays are not supported by this target framework.");
#endif
                case ArrowTypeId.Interval:
                case ArrowTypeId.Map:
                default:
                    throw new NotSupportedException($"An ArrowArray cannot be built for type {data.DataType.TypeId}.");
            }
        }

        // Binary
        public static StringArray BuildArray(IEnumerable<string> values)
        {
            return new StringArray.Builder().AppendRange(values).Build();
        }

        public static BinaryArray BuildArray(IEnumerable<byte[]> values)
        {
            return new BinaryArray.Builder().AppendRange(values).Build();
        }

        // Boolean
        public static BooleanArray BuildArray(IEnumerable<bool> values)
        {
            return new BooleanArray.Builder().AppendRange(values).Build();
        }

        public static BooleanArray BuildArray(IEnumerable<bool?> values)
        {
            return new BooleanArray.Builder().AppendRange(values).Build();
        }

        // Integers
        public static Int8Array BuildArray(IEnumerable<sbyte> values)
        {
            return new Int8Array.Builder().AppendRange(values).Build();
        }

        public static Int8Array BuildArray(IEnumerable<sbyte?> values)
        {
            return new Int8Array.Builder().AppendRange(values).Build();
        }

        public static Int16Array BuildArray(IEnumerable<short> values)
        {
            return new Int16Array.Builder().AppendRange(values).Build();
        }

        public static Int16Array BuildArray(IEnumerable<short?> values)
        {
            return new Int16Array.Builder().AppendRange(values).Build();
        }

        public static Int32Array BuildArray(IEnumerable<int> values)
        {
            return new Int32Array.Builder().AppendRange(values).Build();
        }

        public static Int32Array BuildArray(IEnumerable<int?> values)
        {
            return new Int32Array.Builder().AppendRange(values).Build();
        }

        public static Int64Array BuildArray(IEnumerable<long> values)
        {
            return new Int64Array.Builder().AppendRange(values).Build();
        }

        public static Int64Array BuildArray(IEnumerable<long?> values)
        {
            return new Int64Array.Builder().AppendRange(values).Build();
        }

        // Unsigned Integers
        public static UInt16Array BuildArray(IEnumerable<ushort> values)
        {
            return new UInt16Array.Builder().AppendRange(values).Build();
        }

        public static UInt16Array BuildArray(IEnumerable<ushort?> values)
        {
            return new UInt16Array.Builder().AppendRange(values).Build();
        }

        public static UInt32Array BuildArray(IEnumerable<uint> values)
        {
            return new UInt32Array.Builder().AppendRange(values).Build();
        }

        public static UInt32Array BuildArray(IEnumerable<uint?> values)
        {
            return new UInt32Array.Builder().AppendRange(values).Build();
        }

        public static UInt64Array BuildArray(IEnumerable<ulong> values)
        {
            return new UInt64Array.Builder().AppendRange(values).Build();
        }

        public static UInt64Array BuildArray(IEnumerable<ulong?> values)
        {
            return new UInt64Array.Builder().AppendRange(values).Build();
        }

        // Floats
        public static FloatArray BuildArray(IEnumerable<float> values)
        {
            return new FloatArray.Builder().AppendRange(values).Build();
        }

        public static FloatArray BuildArray(IEnumerable<float?> values)
        {
            return new FloatArray.Builder().AppendRange(values).Build();
        }

        public static DoubleArray BuildArray(IEnumerable<double> values)
        {
            return new DoubleArray.Builder().AppendRange(values).Build();
        }

        public static DoubleArray BuildArray(IEnumerable<double?> values)
        {
            return new DoubleArray.Builder().AppendRange(values).Build();
        }

        public static Decimal256Array BuildArray(IEnumerable<decimal> values)
        {
            return new Decimal256Array.Builder(Decimal256Type.Default).AppendRange(values).Build();
        }

        public static Decimal256Array BuildArray(IEnumerable<decimal?> values)
        {
            return new Decimal256Array.Builder(Decimal256Type.Default).AppendRange(values).Build();
        }
    }
}
