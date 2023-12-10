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
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Apache.Arrow
{
    internal static class ArrayPoolExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ArrayLease RentReturn(this ArrayPool<byte> pool, int length, out Memory<byte> buffer)
        {
            byte[] array = pool.Rent(length);
            buffer = array.AsMemory(0, length);
            return new ArrayLease(pool, array);
        }

        internal struct ArrayLease : IDisposable
        {
            private readonly ArrayPool<byte> _pool;
            private byte[] _array;

            public ArrayLease(ArrayPool<byte> pool, byte[] array)
            {
                _pool = pool;
                _array = array;
            }

            public void Dispose()
            {
                if (_array != null)
                {
                    _pool.Return(_array);
                    _array = null;
                }
            }
        }
    }
}
