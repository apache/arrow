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
    static class ArrowArrayBuilderFactory
    {
        internal static IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> Build(IArrowType dataType)
        {
            switch (dataType.TypeId)
            {
                case ArrowTypeId.Boolean:
                    return new BooleanArray.Builder();
                case ArrowTypeId.UInt8:
                    return new UInt8Array.Builder();
                case ArrowTypeId.Int8:
                    return new Int8Array.Builder();
                case ArrowTypeId.UInt16:
                    return new UInt16Array.Builder();
                case ArrowTypeId.Int16:
                    return new Int16Array.Builder();
                case ArrowTypeId.UInt32:
                    return new UInt32Array.Builder();
                case ArrowTypeId.Int32:
                    return new Int32Array.Builder();
                case ArrowTypeId.UInt64:
                    return new UInt64Array.Builder();
                case ArrowTypeId.Int64:
                    return new Int64Array.Builder();
                case ArrowTypeId.HalfFloat:
#if NET5_0_OR_GREATER
                    return new HalfFloatArray.Builder();
#else
                    throw new NotSupportedException("Half-float arrays are not supported by this target framework.");
#endif
                case ArrowTypeId.Float:
                    return new FloatArray.Builder();
                case ArrowTypeId.Double:
                    return new DoubleArray.Builder();
                case ArrowTypeId.String:
                    return new StringArray.Builder();
                case ArrowTypeId.Binary:
                    return new BinaryArray.Builder();
                case ArrowTypeId.Timestamp:
                    return new TimestampArray.Builder();
                case ArrowTypeId.Date64:
                    return new Date64Array.Builder();
                case ArrowTypeId.Date32:
                    return new Date32Array.Builder();
                case ArrowTypeId.Time32:
                    return new Time32Array.Builder(dataType as Time32Type);
                case ArrowTypeId.Time64:
                    return new Time64Array.Builder(dataType as Time64Type);
                case ArrowTypeId.List:
                    return new ListArray.Builder(dataType as ListType);
                case ArrowTypeId.Decimal128:
                    return new Decimal128Array.Builder(dataType as Decimal128Type);
                case ArrowTypeId.Decimal256:
                    return new Decimal256Array.Builder(dataType as Decimal256Type);
                case ArrowTypeId.Struct:
                case ArrowTypeId.Union:
                case ArrowTypeId.Dictionary:
                case ArrowTypeId.FixedSizedBinary:
                case ArrowTypeId.Interval:
                case ArrowTypeId.Map:
                default:
                    throw new NotSupportedException($"An ArrowArrayBuilder cannot be built for type {dataType.TypeId}.");
            }
        }
    }
}
