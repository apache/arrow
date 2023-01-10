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
using System.Linq.Expressions;
using System.Reflection;

namespace Apache.Arrow.Ipc.Compression
{
    /// <summary>
    /// Decompressor for the ZSTD compression codec that uses reflection so that ZSTD decompression
    /// support can be optional and not require package dependencies for all users.
    /// </summary>
    internal sealed class ZstdDecompressor : IDecompressor
    {
        private readonly object _decompressor;
        private readonly Func<ReadOnlyMemory<byte>, Memory<byte>, int> _decompress;

        public ZstdDecompressor()
        {
#if (NETSTANDARD2_0_OR_GREATER || NETCOREAPP3_1_OR_GREATER)
            try {
                var decompressorType = GetDecompressorType();
                _decompressor = Activator.CreateInstance(decompressorType);

                // Create an expression equivalent to:
                // _decompressor.Unwrap(source.Span, dest.Span, bufferSizePrecheck: true);

                var sourceParam = Expression.Parameter(typeof(ReadOnlyMemory<byte>));
                var destParam = Expression.Parameter(typeof(Memory<byte>));

                var instance = Expression.Constant(_decompressor);
                var sourceSpan = Expression.PropertyOrField(sourceParam, "Span");
                var destSpan = Expression.PropertyOrField(destParam, "Span");
                var bufferSizePrecheck = Expression.Constant(true);

                var unwrapMethod = GetDecompressorUnwrapMethod(decompressorType);
                var unwrapCall = Expression.Call(instance, unwrapMethod, sourceSpan, destSpan, bufferSizePrecheck);

                _decompress = Expression.Lambda<Func<ReadOnlyMemory<byte>, Memory<byte>, int>>(
                    unwrapCall, new [] {sourceParam, destParam}
                ).Compile();
            }
            catch (FileNotFoundException exc)
            {
                throw new Exception($"Error finding ZSTD decompression dependency ({exc.Message.Trim()}). " +
                                    "ZSTD decompression support requires the ZstdNet package to be installed");
            }
#else
            throw new NotSupportedException(
                "ZSTD decompression support requires at least netstandard 2.0 or netcoreapp 3.1, and the ZstdNet package to be installed");
#endif
        }

        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            return _decompress(source, destination);
        }

        public void Dispose()
        {
            ((IDisposable) _decompressor).Dispose();
        }

#if (NETSTANDARD2_0_OR_GREATER || NETCOREAPP3_1_OR_GREATER)
        private static Type GetDecompressorType()
        {
            return Type.GetType("ZstdNet.Decompressor, ZstdNet", throwOnError: true);
        }

        private static MethodInfo GetDecompressorUnwrapMethod(Type decompressorType)
        {
            var unwrapMethod = decompressorType.GetMethod(
                "Unwrap", BindingFlags.Instance | BindingFlags.Public, Type.DefaultBinder,
                new Type[] {typeof(ReadOnlySpan<byte>), typeof(Span<byte>), typeof(bool)}, null);
            if (unwrapMethod == null)
            {
                throw new Exception("Failed to find Unwrap method on ZstdNet.Decompressor");
            }
            return unwrapMethod;
        }
#endif
    }
}
