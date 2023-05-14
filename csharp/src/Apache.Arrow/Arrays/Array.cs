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
using System.Runtime.CompilerServices;
using Apache.Arrow.Arrays;
using Apache.Arrow.Builder;

namespace Apache.Arrow
{
    public abstract class Array : IArrowArray
    {
        public ArrayData Data { get; }

        protected Array(ArrayData data)
        {
            Data = data ?? throw new ArgumentNullException(nameof(data));
        }

        public int Length => Data.Length;

        public int Offset => Data.Offset;

        public int NullCount => Data.NullCount;

        public ArrowBuffer NullBitmapBuffer => Data.Buffers[0];

        public virtual void Accept(IArrowArrayVisitor visitor)
        {
            Accept(this, visitor);
        }

        TArray IArrowArray.As<TArray>() => As<TArray>();
        TArray As<TArray>() where TArray : IArrowArray
        {
            if (this is not TArray casted)
            {
                throw new InvalidOperationException($"Cannot cast {this} as {typeof(TArray)}");
            }
            return casted;
        }

        public bool IsValid(int index) =>
            NullCount == 0 || NullBitmapBuffer.IsEmpty || BitUtility.GetBit(NullBitmapBuffer.Span, index + Offset);

        public bool IsNull(int index) => !IsValid(index);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Accept<T>(T array, IArrowArrayVisitor visitor)
            where T : class, IArrowArray
        {
            switch (visitor)
            {
                case IArrowArrayVisitor<T> typedVisitor:
                    typedVisitor.Visit(array);
                    break;
                default:
                    visitor.Visit(array);
                    break;
            }
        }

        public IArrowArray Slice(int offset, int length)
        {
            if (offset > Length)
            {
                throw new ArgumentException($"Offset {offset} cannot be greater than Length {Length} for Array.Slice");
            }

            length = Math.Min(Data.Length - offset, length);
            offset += Offset;

            ArrayData newData = Data.Slice(offset, length);
            return ArrayBuilderFactory.MakeArray(newData);
        }

        /// <summary>
        /// Get non nullable arrow scalar from array at index
        /// </summary>
        /// <param name="index">value index</param>
        /// <returns><see cref="IScalar"/></returns>
        public virtual IScalar GetScalar(int index)
        {
            switch (Data.DataType.TypeId)
            {
                case Types.ArrowTypeId.Binary:
                    return As<BinaryArray>().GetScalar(index);
                case Types.ArrowTypeId.FixedSizedBinary:
                    return As<FixedSizeBinaryArray>().GetScalar(index);
                case Types.ArrowTypeId.String:
                    return As<StringArray>().GetScalar(index);
                case Types.ArrowTypeId.List:
                    return As<ListArray>().GetScalar(index);
                case Types.ArrowTypeId.Struct:
                    return As<StructArray>().GetScalar(index);
                default:
                    throw new NotSupportedException($"Cannot get scalar from array of type {Data.DataType}");
            }
        }

        /// <summary>
        /// Get nullable arrow scalar from array at index
        /// </summary>
        /// <param name="index">value index</param>
        /// <returns><see cref="INullableScalar"/></returns>
        public INullableScalar GetNullableScalar(int index)
            => IsValid(index) ?
                new NullableScalar(Data.DataType, false, null) :
                new NullableScalar(Data.DataType, true, GetScalar(index));

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Data.Dispose();
            }
        }
    }
}
