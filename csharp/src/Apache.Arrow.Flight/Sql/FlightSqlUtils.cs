#nullable enable
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using Apache.Arrow.Flight.Internal;
using Arrow.Flight.Protocol.Sql;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Apache.Arrow.Flight.Sql
{
    /// <summary>
    /// Helper methods for doing common Flight Sql tasks and conversions
    /// </summary>
    public class FlightSqlUtils
    {
        public static readonly FlightActionType FlightSqlCreatePreparedStatement = new("CreatePreparedStatement",
            "Creates a reusable prepared statement resource on the server. \n" +
            "Request Message: ActionCreatePreparedStatementRequest\n" +
            "Response Message: ActionCreatePreparedStatementResult");

        public static readonly FlightActionType FlightSqlClosePreparedStatement = new("ClosePreparedStatement",
            "Closes a reusable prepared statement resource on the server. \n" +
            "Request Message: ActionClosePreparedStatementRequest\n" +
            "Response Message: N/A");

        /// <summary>
        /// List of possible actions
        /// </summary>
        public static readonly List<FlightActionType> FlightSqlActions = new()
        {
            FlightSqlCreatePreparedStatement,
            FlightSqlClosePreparedStatement
        };

        /// <summary>
        /// Helper to parse {@link com.google.protobuf.Any} objects to the specific protobuf object.
        /// </summary>
        /// <param name="source">the raw bytes source value.</param>
        /// <returns>the materialized protobuf object.</returns>
        public static Any Parse(ByteString source)
        {
            return Any.Parser.ParseFrom(source);
        }

        /// <summary>
        /// Helper to unpack {@link com.google.protobuf.Any} objects to the specific protobuf object.
        /// </summary>
        /// <param name="source">the parsed Source value.</param>
        /// <typeparam name="T">IMessage</typeparam>
        /// <returns>the materialized protobuf object.</returns>
        public static T Unpack<T>(Any source) where T : IMessage, new()
        {
            return source.Unpack<T>();
        }

        /// <summary>
        /// Helper to parse and unpack {@link com.google.protobuf.Any} objects to the specific protobuf object.
        /// </summary>
        /// <param name="source">the raw bytes source value.</param>
        /// <typeparam name="T">IMessage</typeparam>
        /// <returns>the materialized protobuf object.</returns>
        public static T ParseAndUnpack<T>(ByteString source) where T : IMessage, new()
        {
            return Unpack<T>(Parse(source));
        }
    }

    /// <summary>
    /// A set of helper functions for converting encoded commands to IMessage types
    /// </summary>
    public static class FlightSqlExtensions
    {
        private static Any ParsedCommand(this FlightDescriptor descriptor)
        {
            return FlightSqlUtils.Parse(descriptor.Command);
        }

        private static IMessage UnpackMessage(this Any command)
        {
            if (command.Is(CommandStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<CommandStatementQuery>(command);
            if (command.Is(CommandPreparedStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<CommandPreparedStatementQuery>(command);
            if (command.Is(CommandGetCatalogs.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetCatalogs>(command);
            if (command.Is(CommandGetDbSchemas.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetDbSchemas>(command);
            if (command.Is(CommandGetTables.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetTables>(command);
            if (command.Is(CommandGetTableTypes.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetTableTypes>(command);
            if (command.Is(CommandGetSqlInfo.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetSqlInfo>(command);
            if (command.Is(CommandGetPrimaryKeys.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetPrimaryKeys>(command);
            if (command.Is(CommandGetExportedKeys.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetExportedKeys>(command);
            if (command.Is(CommandGetImportedKeys.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetImportedKeys>(command);
            if (command.Is(CommandGetCrossReference.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetCrossReference>(command);
            if (command.Is(CommandGetXdbcTypeInfo.Descriptor))
                return FlightSqlUtils.Unpack<CommandGetXdbcTypeInfo>(command);
            if (command.Is(TicketStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<TicketStatementQuery>(command);
            if (command.Is(TicketStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<TicketStatementQuery>(command);
            if (command.Is(CommandStatementUpdate.Descriptor))
                return FlightSqlUtils.Unpack<CommandStatementUpdate>(command);
            if (command.Is(CommandPreparedStatementUpdate.Descriptor))
                return FlightSqlUtils.Unpack<CommandPreparedStatementUpdate>(command);
            if (command.Is(CommandPreparedStatementQuery.Descriptor))
                return FlightSqlUtils.Unpack<CommandPreparedStatementQuery>(command);

            throw new ArgumentException("The defined request is invalid.");
        }

        /// <summary>
        /// Extracts a command from a FlightDescriptor
        /// </summary>
        /// <param name="descriptor"></param>
        /// <returns>An IMessage that has been parsed and unpacked</returns>
        public static IMessage? ParsedAndUnpackedMessage(this FlightDescriptor descriptor)
        {
            try
            {
                return descriptor.ParsedCommand().UnpackMessage();
            }
            catch (ArgumentException)
            {
                return null;
            }
        }

        public static ByteString ToByteString(this Schema schema)
        {
            return SchemaWriter.SerializeSchema(schema);
        }

        public static ByteString Serialize(this IBufferMessage message)
        {
            int size = message.CalculateSize();
            var writer = new ArrayBufferWriter<byte>(size);
            message.WriteTo(writer);
            var schemaBytes = writer.WrittenSpan;
            return ByteString.CopyFrom(schemaBytes);
        }
    }

    // TODO: Remove this class if .net standard 2.0 support is dropped (class was introduced in .Net standard 2.1 and .Net core 3.0)
    // Copied from https://github.com/dotnet/runtime/blob/main/src/libraries/Common/src/System/Buffers/ArrayBufferWriter.cs
    // Licensed to the .NET Foundation under one or more agreements.
    // The .NET Foundation licenses this file to you under the MIT license.
    /// <summary>
    /// Represents a heap-based, array-backed output sink into which <typeparam name="T"/> data can be written.
    /// </summary>
#if NETSTANDARD2_0
    sealed class ArrayBufferWriter<T> : IBufferWriter<T>
    {
        // Copy of Array.MaxLength.
        // Used by projects targeting .NET Framework.
        private const int ArrayMaxLength = 0x7FFFFFC7;

        private const int DefaultInitialBufferSize = 256;

        private T[] _buffer;
        private int _index;


        /// <summary>
        /// Creates an instance of an <see cref="ArrayBufferWriter{T}"/>, in which data can be written to,
        /// with the default initial capacity.
        /// </summary>
        public ArrayBufferWriter()
        {
            _buffer = System.Array.Empty<T>();
            _index = 0;
        }

        /// <summary>
        /// Creates an instance of an <see cref="ArrayBufferWriter{T}"/>, in which data can be written to,
        /// with an initial capacity specified.
        /// </summary>
        /// <param name="initialCapacity">The minimum capacity with which to initialize the underlying buffer.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="initialCapacity"/> is not positive (i.e. less than or equal to 0).
        /// </exception>
        public ArrayBufferWriter(int initialCapacity)
        {
            if (initialCapacity <= 0)
                throw new ArgumentException(null, nameof(initialCapacity));

            _buffer = new T[initialCapacity];
            _index = 0;
        }

        /// <summary>
        /// Returns the data written to the underlying buffer so far, as a <see cref="ReadOnlyMemory{T}"/>.
        /// </summary>
        public ReadOnlyMemory<T> WrittenMemory => _buffer.AsMemory(0, _index);

        /// <summary>
        /// Returns the data written to the underlying buffer so far, as a <see cref="ReadOnlySpan{T}"/>.
        /// </summary>
        public ReadOnlySpan<T> WrittenSpan => _buffer.AsSpan(0, _index);

        /// <summary>
        /// Returns the amount of data written to the underlying buffer so far.
        /// </summary>
        public int WrittenCount => _index;

        /// <summary>
        /// Returns the total amount of space within the underlying buffer.
        /// </summary>
        public int Capacity => _buffer.Length;

        /// <summary>
        /// Returns the amount of space available that can still be written into without forcing the underlying buffer to grow.
        /// </summary>
        public int FreeCapacity => _buffer.Length - _index;

        /// <summary>
        /// Clears the data written to the underlying buffer.
        /// </summary>
        /// <remarks>
        /// You must clear the <see cref="ArrayBufferWriter{T}"/> before trying to re-use it.
        /// </remarks>
        public void Clear()
        {
            Debug.Assert(_buffer.Length >= _index);
            _buffer.AsSpan(0, _index).Clear();
            _index = 0;
        }

        /// <summary>
        /// Notifies <see cref="IBufferWriter{T}"/> that <paramref name="count"/> amount of data was written to the output <see cref="Span{T}"/>/<see cref="Memory{T}"/>
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="count"/> is negative.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Thrown when attempting to advance past the end of the underlying buffer.
        /// </exception>
        /// <remarks>
        /// You must request a new buffer after calling Advance to continue writing more data and cannot write to a previously acquired buffer.
        /// </remarks>
        public void Advance(int count)
        {
            if (count < 0)
                throw new ArgumentException(null, nameof(count));

            if (_index > _buffer.Length - count)
                ThrowInvalidOperationException_AdvancedTooFar(_buffer.Length);

            _index += count;
        }

        /// <summary>
        /// Returns a <see cref="Memory{T}"/> to write to that is at least the requested length (specified by <paramref name="sizeHint"/>).
        /// If no <paramref name="sizeHint"/> is provided (or it's equal to <code>0</code>), some non-empty buffer is returned.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sizeHint"/> is negative.
        /// </exception>
        /// <remarks>
        /// This will never return an empty <see cref="Memory{T}"/>.
        /// </remarks>
        /// <remarks>
        /// There is no guarantee that successive calls will return the same buffer or the same-sized buffer.
        /// </remarks>
        /// <remarks>
        /// You must request a new buffer after calling Advance to continue writing more data and cannot write to a previously acquired buffer.
        /// </remarks>
        public Memory<T> GetMemory(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            Debug.Assert(_buffer.Length > _index);
            return _buffer.AsMemory(_index);
        }

        /// <summary>
        /// Returns a <see cref="Span{T}"/> to write to that is at least the requested length (specified by <paramref name="sizeHint"/>).
        /// If no <paramref name="sizeHint"/> is provided (or it's equal to <code>0</code>), some non-empty buffer is returned.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="sizeHint"/> is negative.
        /// </exception>
        /// <remarks>
        /// This will never return an empty <see cref="Span{T}"/>.
        /// </remarks>
        /// <remarks>
        /// There is no guarantee that successive calls will return the same buffer or the same-sized buffer.
        /// </remarks>
        /// <remarks>
        /// You must request a new buffer after calling Advance to continue writing more data and cannot write to a previously acquired buffer.
        /// </remarks>
        public Span<T> GetSpan(int sizeHint = 0)
        {
            CheckAndResizeBuffer(sizeHint);
            Debug.Assert(_buffer.Length > _index);
            return _buffer.AsSpan(_index);
        }

        private void CheckAndResizeBuffer(int sizeHint)
        {
            if (sizeHint < 0)
                throw new ArgumentException(nameof(sizeHint));

            if (sizeHint == 0)
            {
                sizeHint = 1;
            }

            if (sizeHint > FreeCapacity)
            {
                int currentLength = _buffer.Length;

                // Attempt to grow by the larger of the sizeHint and double the current size.
                int growBy = Math.Max(sizeHint, currentLength);

                if (currentLength == 0)
                {
                    growBy = Math.Max(growBy, DefaultInitialBufferSize);
                }

                int newSize = currentLength + growBy;

                if ((uint)newSize > int.MaxValue)
                {
                    // Attempt to grow to ArrayMaxLength.
                    uint needed = (uint)(currentLength - FreeCapacity + sizeHint);
                    Debug.Assert(needed > currentLength);

                    if (needed > ArrayMaxLength)
                    {
                        ThrowOutOfMemoryException(needed);
                    }

                    newSize = ArrayMaxLength;
                }

                System.Array.Resize(ref _buffer, newSize);
            }

            Debug.Assert(FreeCapacity > 0 && FreeCapacity >= sizeHint);
        }

        private static void ThrowInvalidOperationException_AdvancedTooFar(int capacity)
        {
            throw new InvalidOperationException($"Cannot advance past the end of the buffer, which has a size of {capacity}");
        }

        private static void ThrowOutOfMemoryException(uint capacity)
        {
            throw new OutOfMemoryException($"Cannot allocate a buffer of size {capacity}");
        }
    }
#endif
}
