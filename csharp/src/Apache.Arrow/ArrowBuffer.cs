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
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public readonly partial struct ArrowBuffer: IEquatable<ArrowBuffer>
    {
        public static ArrowBuffer Empty => new ArrowBuffer(Memory<byte>.Empty);

        private ArrowBuffer(Memory<byte> data)
        {
            Memory = data;
        }

        public ReadOnlyMemory<byte> Memory { get; }

        public bool IsEmpty => Memory.IsEmpty;

        public int Length => Memory.Length;

        public ReadOnlySpan<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Memory.Span;
        }

        public ArrowBuffer Clone(MemoryPool pool = default)
        {
            return new Builder<byte>(Span.Length)
                .Append(Span)
                .Build(pool);
        }

        public bool Equals(ArrowBuffer other)
        {
            return Span.SequenceEqual(other.Span);
        }
    }
}
