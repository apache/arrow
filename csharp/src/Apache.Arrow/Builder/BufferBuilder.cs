using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class BufferBuilder : IBufferBuilder
    {
        private const int DefaultCapacity = 64;
        public int ByteLength { get; private set; }
        public int BitOffset { get; private set; }

        public Memory<byte> Memory { get; private set; }

        /// <summary>
        /// Creates an instance of the <see cref="BufferBuilder"/> class.
        /// </summary>
        /// <param name="capacity">Number of bytes of initial capacity to reserve.</param>
        public BufferBuilder(int capacity = DefaultCapacity)
        {
            Memory = new byte[capacity];

            ByteLength = 0;
            BitOffset = 0;
        }

        public IBufferBuilder AppendBit(bool bit)
        {
            UncheckedAppendBit(bit);

            if (BitOffset == 8)
            {
                BitOffset = 0;
                ByteLength++;
                // Ensure current and another byte for next append
                EnsureBytes(ByteLength + 1);
            }
            return this;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void UncheckedAppendBit(bool bit)
        {
            BitUtility.SetBit(ref Memory.Span[ByteLength], BitOffset, bit);
            BitOffset++;
        }

        public IBufferBuilder AppendBits(ReadOnlySpan<bool> bits)
        {
            if (BitOffset > 0)
            {
                int available = 8 - BitOffset;

                if (bits.Length < available)
                {
                    foreach (bool bit in bits)
                        UncheckedAppendBit(bit);

                    bits = ReadOnlySpan<bool>.Empty;
                }
                else
                {
                    foreach (bool bit in bits.Slice(0, available))
                        UncheckedAppendBit(bit);

                    BitOffset = 0;
                    ByteLength++;

                    bits = bits.Slice(available);
                }
            }

            if (bits.Length > 0)
            {
                int byteEnd = bits.Length / 8;
                int bitEnd = byteEnd * 8;

                if (byteEnd > 0)
                {
                    // Ensure byte length
                    EnsureAdditionalBytes(byteEnd);

                    // Raw Span copy to memory
                    BitUtility.SetBits(Memory.Span.Slice(ByteLength, byteEnd), bits.Slice(0, bitEnd));

                    ByteLength += byteEnd;

                    bits = bits.Slice(bitEnd);
                }
                
                if (bits.Length > 0)
                {
                    // Fill byte buffer with last unfilled
                    foreach (bool bit in bits)
                        UncheckedAppendBit(bit);
                }
            }

            return this;
        }

        public IBufferBuilder AppendBits(bool value, int count)
        {
            // by default bit values are false
            if (!value)
                return AppendEmptyBits(count);

            int remainderBits = BitOffset == 0 ? 0 : Math.Min(count, 8 - BitOffset);
            int wholeBytes = (count - remainderBits) / 8;
            int trailingBits = count - remainderBits - (wholeBytes * 8);
            int newBytes = (trailingBits > 0) ? wholeBytes + 1 : wholeBytes;

            EnsureAdditionalBytes(newBytes);

            var span = Memory.Span;

            // Fill remaining bits in current bit offset
            for (int i = 0; i < remainderBits; i++)
                UncheckedAppendBit(value);

            if (BitOffset == 8)
            {
                BitOffset = 0;
                ByteLength++;
            }

            // Bulk write true or false bytes
            if (wholeBytes > 0)
            {
                var fill = (byte)0xFF;
                span.Slice(ByteLength, wholeBytes).Fill(fill);
            }

            ByteLength += wholeBytes;

            // Write remaining bits
            for (int i = 0; i < trailingBits; i++)
                UncheckedAppendBit(value);

            return this;
        }

        public IBufferBuilder AppendEmptyBits(int count)
        {
            if (BitOffset == 0)
            {
                int end = count / 8;
                EnsureAdditionalBytes(end);
                ByteLength += end;
                BitOffset = count - end * 8;
            }
            else
            {
                int remainderBits = Math.Min(count, 8 - BitOffset);
                int wholeBytes = (count - remainderBits) / 8;
                int trailingBits = count - remainderBits - (wholeBytes * 8);
                int newBytes = (trailingBits > 0) ? wholeBytes + 1 : wholeBytes;

                EnsureAdditionalBytes(newBytes);

                ByteLength += wholeBytes;
                BitOffset = remainderBits + trailingBits;
            }

            return this;
        }

        public IBufferBuilder AppendByte(byte value)
        {
            if (BitOffset > 0)
            {
                // Convert byte to bits
                Span<bool> bits = stackalloc bool[8];
                BitUtility.ByteToBits(value, bits);

                AppendBits(bits);
            }
            else
            {
                EnsureAdditionalBytes(1);
                // Raw add to memory
                Memory.Span[ByteLength] = value;
                ByteLength++;
            }

            return this;
        }

        public IBufferBuilder AppendFixedSizeBytes(
            ICollection<byte[]> bytes, int fixedSize, Span<bool> validity, out int nullCount
            )
        {
            int i = 0;
            int offset = ByteLength;
            int _nullCount = 0;
            EnsureAdditionalBytes(bytes.Count * fixedSize);

            foreach (byte[] value in bytes)
            {
                if (value == null)
                    _nullCount++;
                else
                {
                    // Copy to memory
                    try
                    {
                        value.CopyTo(Memory.Span.Slice(offset, fixedSize));
                    }
                    catch (System.ArgumentException)
                    {
                        // Destination is too short. (Parameter 'destination')
                        value.AsSpan().Slice(0, fixedSize).CopyTo(Memory.Span.Slice(offset, fixedSize));
                    }
                    validity[i] = true;
                }
                offset += fixedSize;
                i++;
            }

            ByteLength = offset;
            nullCount = _nullCount;
            return this;
        }

        public IBufferBuilder AppendBytes(ReadOnlySpan<byte> bytes)
        {
            if (BitOffset == 0)
            {
                EnsureAdditionalBytes(bytes.Length);
                // Raw Span copy to memory
                bytes.CopyTo(Memory.Span.Slice(ByteLength, bytes.Length));
                ByteLength += bytes.Length;
            }
            else
            {
                // Convert Bytes to Bits streamed in batchsize = 128 bytes
                int offset = 0;
                while (offset < bytes.Length)
                {
                    int remainingBytes = bytes.Length - offset;
                    int bufferLength = Math.Min(128, remainingBytes);

                    // Append batch bits
                    var bits = BitUtility.BytesToBits(bytes.Slice(offset, bufferLength));
                    AppendBits(bits);
                    offset += bufferLength;
                }
            }

            return this;
        }

        public IBufferBuilder AppendEmptyBytes(int count)
        {
            if (BitOffset == 0)
            {
                ByteLength += count;
                return this;
            }
            return AppendEmptyBits(count * 8);
        }

        public IBufferBuilder AppendValue(bool value) => AppendBit(value);
        public IBufferBuilder AppendValue(byte value) => AppendByte(value);
        public IBufferBuilder AppendValue<T>(T value) where T : struct
            => AppendValues(TypeReflection.CreateReadOnlySpan(ref value));

        public IBufferBuilder AppendValues(ReadOnlySpan<bool> values) => AppendBits(values);
        public IBufferBuilder AppendValues(ReadOnlySpan<byte> values) => AppendBytes(values);
        public IBufferBuilder AppendValues<T>(ReadOnlySpan<T> values) where T : struct
        {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(values);
            return AppendBytes(bytes);
        }
        public IBufferBuilder AppendValues(bool value, int count) => AppendBits(value, count);
        public IBufferBuilder AppendValues<T>(T value, int count) where T : struct
        {
            Span<T> span = new T[count];

            for (int i = 0; i < count; i++)
                span[i] = value;

            return AppendValues<T>(span);
        }

        internal IBufferBuilder ReserveAdditionalBytes(int numBytes)
        {
            EnsureAdditionalBytes(numBytes);
            return this;
        }

        internal IBufferBuilder ResizeBytes(int numBytes)
        {
            EnsureBytes(numBytes);
            ByteLength = numBytes;
            BitOffset = 0;
            return this;
        }

        public IBufferBuilder Clear()
        {
            Memory.Span.Fill(default);
            ByteLength = 0;
            BitOffset = 0;
            return this;
        }

        public ArrowBuffer Build(MemoryAllocator allocator = default) => Build(64, allocator);

        public ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default)
        {
            var byteLength = BitOffset > 0 ? ByteLength + 1 : ByteLength;

            int bufferLength = checked((int)BitUtility.RoundUpToMultiplePowerOfTwo(byteLength, byteSize));

            MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
            IMemoryOwner<byte> memoryOwner = memoryAllocator.Allocate(bufferLength);
            Memory.Slice(0, byteLength).CopyTo(memoryOwner.Memory);

            return new ArrowBuffer(memoryOwner);
        }
         
        public void EnsureAdditionalBytes(int numBytes) => EnsureBytes(checked(ByteLength + numBytes));

        internal void EnsureBytes(int numBytes)
        {
            if (numBytes > Memory.Length)
            {
                int twice = checked(Memory.Length * 2);

                Reallocate(twice < numBytes ? BitUtility.NextPowerOfTwo(numBytes) : twice);
            }
        }

        private void Reallocate(int numBytes)
        {
            var memory = new Memory<byte>(new byte[numBytes]);
            Memory.CopyTo(memory);

            Memory = memory;
        }
    }

    public class ValueBufferBuilder : BufferBuilder, IValueBufferBuilder
    {
        public int ValueBitSize { get; }
        public int ValueLength => (ByteLength * 8 + BitOffset) / ValueBitSize;

        public ValueBufferBuilder(int valueBitSize, int capacity = 32) : base(capacity * (valueBitSize + 7) / 8)
        {
            ValueBitSize = valueBitSize;
        }

        public IValueBufferBuilder Ensure(int capacity)
        {
            EnsureBytes((capacity * ValueBitSize + 7) / 8);
            return this;
        }

        public IValueBufferBuilder Reserve(int additionnalCapacity)
        {
            ReserveAdditionalBytes((additionnalCapacity * ValueBitSize + 7) / 8);
            return this;
        }

        public IValueBufferBuilder Resize(int capacity)
        {
            ResizeBytes((capacity * ValueBitSize + 7) / 8);
            return this;
        }
    }

    public class ValueBufferBuilder<T> : ValueBufferBuilder, IPrimitiveBufferBuilder<T> where T : struct
    {
        public ValueBufferBuilder(int capacity = 32) : this((TypeReflection<T>.ArrowType as FixedWidthType).BitWidth, capacity)
        {
        }

        public ValueBufferBuilder(int bitWidth, int capacity = 32) : base(bitWidth, capacity)
        {
        }

        public IPrimitiveBufferBuilder<T> AppendValue(T value)
        {
            base.AppendValue(value);
            return this;
        }
        public IPrimitiveBufferBuilder<T> AppendValues(ReadOnlySpan<T> values)
        {
            base.AppendValues(values);
            return this;
        }
        public IPrimitiveBufferBuilder<T> AppendValues(T value, int count)
        {
            base.AppendValues(value, count);
            return this;
        }
    }
}

