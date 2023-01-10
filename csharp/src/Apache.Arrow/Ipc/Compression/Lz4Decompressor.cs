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
    /// Decompressor for the LZ4 Frame compression codec that uses reflection so that LZ4 decompression
    /// support can be optional and not require package dependencies for all users.
    /// </summary>
    internal sealed class Lz4Decompressor : IDecompressor
    {
        private readonly Func<ReadOnlyMemory<byte>, Memory<byte>, long> _decompress;

        public Lz4Decompressor()
        {
#if (NETSTANDARD2_0_OR_GREATER || NETCOREAPP3_1_OR_GREATER)
            try {
                // Create a function equivalent to:
                // Stream sourceStream;
                // Stream destStream;
                // Stream decompressedStream;
                // long decompressedLength;
                // try
                // {
                //     sourceStream = source.AsStream();
                //     destStream = destination.AsStream();
                //     decompressedStream = LZ4Stream.Decode(sourceStream, null, false, false);
                //     decompressedStream.CopyTo(destStream);
                //     decompressedLength = dest.Length;
                // }
                // finally
                // {
                //     if (decompressedStream != null) { decompressedStream.Dispose(); }
                //     if (destStream != null) { destStream.Dispose(); }
                //     if (sourceStream != null) { sourceStream.Dispose(); }
                // }
                // return decompressedLength;

                var decodeMethod = GetStreamDecodeMethod();
                var readOnlyMemoryAsStream = GetReadOnlyMemoryAsStreamExtensionMethod();
                var memoryAsStream = GetMemoryAsStreamExtensionMethod();
                var streamCopyTo = GetStreamCopyToMethod();
                var disposeMethod = GetDisposeMethod();

                var sourceParam = Expression.Parameter(typeof(ReadOnlyMemory<byte>));
                var destParam = Expression.Parameter(typeof(Memory<byte>));

                var sourceStream = Expression.Variable(typeof(Stream), "sourceStream");
                var destStream = Expression.Variable(typeof(Stream), "destStream");
                var decompressedStream = Expression.Variable(typeof(Stream), "decompressedStream");
                var decompressedLength = Expression.Variable(typeof(long), "decompressedLength");
                var variables = new[] {sourceStream, destStream, decompressedStream, decompressedLength};

                var assignSourceStream = Expression.Assign(sourceStream, Expression.Call(readOnlyMemoryAsStream, sourceParam));
                var assignDestStream = Expression.Assign(destStream, Expression.Call(memoryAsStream, destParam));

                var decoderSettings = Expression.Constant(null, GetLz4DecoderSettingsType());
                var leaveOpen = Expression.Constant(false);
                var interactive = Expression.Constant(false);
                var getDecompressedStream =
                    Expression.Call(decodeMethod, sourceStream, decoderSettings, leaveOpen, interactive);
                var assignDecompressedStream = Expression.Assign(decompressedStream, getDecompressedStream);

                var copyStream = Expression.Call(
                    decompressedStream, streamCopyTo, destStream);
                var assignDecompressedLength = Expression.Assign(
                    decompressedLength, Expression.PropertyOrField(destStream, "Length"));

                var disposeDecompressed = Expression.IfThen(
                    Expression.NotEqual(decompressedStream, Expression.Constant(null)),
                    Expression.Call(decompressedStream, disposeMethod));
                var disposeDest = Expression.IfThen(
                    Expression.NotEqual(destStream, Expression.Constant(null)),
                    Expression.Call(destStream, disposeMethod));
                var disposeSource = Expression.IfThen(
                    Expression.NotEqual(sourceStream, Expression.Constant(null)),
                    Expression.Call(sourceStream, disposeMethod));

                var tryFinally = Expression.TryFinally(
                    Expression.Block(
                        assignSourceStream,
                        assignDestStream,
                        assignDecompressedStream,
                        copyStream,
                        assignDecompressedLength),
                    Expression.Block(
                        disposeDecompressed,
                        disposeDest,
                        disposeSource)
                );

                _decompress = Expression.Lambda<Func<ReadOnlyMemory<byte>, Memory<byte>, long>>(
                    Expression.Block(
                        variables,
                        tryFinally, decompressedLength),
                    sourceParam, destParam
                ).Compile();
            }
            catch (FileNotFoundException exc)
            {
                throw new Exception($"Error finding LZ4 decompression dependency ({exc.Message.Trim()}). " +
                                    "LZ4 decompression support requires the K4os.Compression.LZ4.Streams and " +
                                    "CommunityToolkit.HighPerformance packages to be installed");
            }
#else
            throw new NotSupportedException(
                "LZ4 decompression support requires at least netstandard 2.0 or netcoreapp 3.1, " +
                 "and the K4os.Compression.LZ4.Streams and CommunityToolkit.HighPerformance packages to be installed");
#endif
        }

        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
        {
            return (int) _decompress(source, destination);
        }

        public void Dispose()
        {
        }

#if (NETSTANDARD2_0_OR_GREATER || NETCOREAPP3_1_OR_GREATER)
        private static Type GetLz4DecoderSettingsType()
        {
            return Type.GetType(
                "K4os.Compression.LZ4.Streams.LZ4DecoderSettings, K4os.Compression.LZ4.Streams", throwOnError: true);
        }

        private static MethodInfo GetStreamDecodeMethod()
        {
            var lz4StreamType = Type.GetType(
                "K4os.Compression.LZ4.Streams.LZ4Stream, K4os.Compression.LZ4.Streams", throwOnError: true);
            var lz4DecoderSettingsType = GetLz4DecoderSettingsType();

            var decodeMethod = lz4StreamType.GetMethod(
                "Decode", BindingFlags.Static | BindingFlags.Public, Type.DefaultBinder,
                new Type[] {typeof(Stream), lz4DecoderSettingsType, typeof(bool), typeof(bool)}, null);
            if (decodeMethod == null)
            {
                throw new Exception("Failed to find Decode method on the LZ4Stream type");
            }

            return decodeMethod;
        }

        private static MethodInfo GetMemoryAsStreamExtensionMethod()
        {
            var memoryExtensions = Type.GetType(
                "CommunityToolkit.HighPerformance.MemoryExtensions, CommunityToolkit.HighPerformance", throwOnError: true);

            var memoryAsStream = memoryExtensions.GetMethod(
                "AsStream", BindingFlags.Static | BindingFlags.Public, Type.DefaultBinder,
                new Type[] {typeof(Memory<byte>)}, null);
            if (memoryAsStream == null)
            {
                throw new Exception("Failed to find AsStream extension method for Memory<byte>");
            }

            return memoryAsStream;
        }

        private static MethodInfo GetReadOnlyMemoryAsStreamExtensionMethod()
        {
            var readOnlyMemoryExtensions = Type.GetType(
                "CommunityToolkit.HighPerformance.ReadOnlyMemoryExtensions, CommunityToolkit.HighPerformance", throwOnError: true);

            var readOnlyMemoryAsStream = readOnlyMemoryExtensions.GetMethod(
                "AsStream", BindingFlags.Static | BindingFlags.Public, Type.DefaultBinder,
                new Type[] {typeof(ReadOnlyMemory<byte>)}, null);
            if (readOnlyMemoryAsStream == null)
            {
                throw new Exception("Failed to find AsStream extension method for ReadOnlyMemory<byte>");
            }

            return readOnlyMemoryAsStream;
        }

        private static MethodInfo GetStreamCopyToMethod()
        {
            var method = typeof(Stream).GetMethod(
                "CopyTo", BindingFlags.Instance | BindingFlags.Public, Type.DefaultBinder, new Type[] {typeof(Stream)}, null);
            if (method == null)
            {
                throw new Exception("Failed to find Stream.CopyTo method");
            }

            return method;
        }

        private static MethodInfo GetDisposeMethod()
        {
            var method = typeof(IDisposable).GetMethod(
                "Dispose", BindingFlags.Instance | BindingFlags.Public, Type.DefaultBinder, Type.EmptyTypes, null);
            if (method == null)
            {
                throw new Exception("Failed to find IDisposable.Dispose method");
            }

            return method;
        }
#endif
    }
}
