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

        public bool IsValid(int index) =>
            NullCount == 0 || NullBitmapBuffer.IsEmpty || BitUtility.GetBit(NullBitmapBuffer.Span, index);

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
    }
}