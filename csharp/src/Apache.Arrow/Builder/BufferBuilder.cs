using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Builder
{
    public class BufferBuilder : IBufferBuilder
    {
        public class BitBuffer
        {
            private const int DefaultCapacity = 8;

            private readonly bool[] _bits;
            public Span<bool> Bits => _bits.AsSpan().Slice(0, Length);

            public int Length { get; private set; }
            public int AvailableLength => Capacity - Length;

            public int Capacity;

            public bool IsFull => Length == Capacity;
            public byte ToByte(ref byte data) => BitUtility.ToByte(ref data, _bits);

            public BitBuffer(int capacity = DefaultCapacity)
            {
                Capacity = capacity;
                _bits = new bool[capacity];
                Length = 0;
            }

            public void Append(bool bit) => _bits[Length++] = bit;
            public void Fill(ReadOnlySpan<bool> bits)
            {
                bits.CopyTo(_bits.AsSpan().Slice(Length, bits.Length));
                Length += bits.Length;
            }

            public void Reset()
            {
                for (int i = 0; i < _bits.Length; i++)
                {
                    _bits[i] = false;
                }
                Length = 0;
            }
        }

        private const int DefaultBatchSize = 65536; // 64 * 1024
        private const int DefaultCapacity = 64;
        public int ByteLength { get; private set; }

        public Memory<byte> Memory { get; private set; }
        public BitBuffer BitOverhead { get; }

        /// <summary>
        /// Creates an instance of the <see cref="BufferBuilder"/> class.
        /// </summary>
        /// <param name="valueBitSize">Number of bits of one value item.</param>
        /// <param name="capacity">Number of items of initial capacity to reserve.</param>
        public BufferBuilder(int capacity = DefaultCapacity)
        {
            Memory = new byte[capacity];
            BitOverhead = new BitBuffer();

            ByteLength = 0;
        }

        private void CommitBitBuffer(bool force = false)
        {
            if (BitOverhead.IsFull || force)
            {
                EnsureAdditionalBytes(1);
                BitOverhead.ToByte(ref Memory.Span[ByteLength]);
                BitOverhead.Reset();
                ByteLength++;
            }
        }

        public IBufferBuilder AppendBit(bool bit)
        {
            BitOverhead.Append(bit);
            CommitBitBuffer();
            return this;
        }

        public IBufferBuilder AppendBits(ReadOnlySpan<bool> bits)
        {
            if (BitOverhead.Length > 0)
            {
                int available = BitOverhead.AvailableLength;

                if (bits.Length > available)
                {
                    // Fill byte buffer
                    BitOverhead.Fill(bits.Slice(0, available));

                    // Commit to memory and reset
                    CommitBitBuffer();

                    bits = bits.Slice(available);
                }
                else
                {
                    // Fill byte buffer
                    BitOverhead.Fill(bits);

                    bits = ReadOnlySpan<bool>.Empty;
                }
            }

            if (bits.Length > 0)
            {
                int byteEnd = bits.Length / 8;
                int bitEnd = byteEnd * 8;

                if (byteEnd > 0)
                {
                    // Ensure byte length
                    EnsureAdditionalBits(byteEnd);

                    // Raw Span copy to memory
                    BitUtility.ToBytes(Memory.Span.Slice(ByteLength, byteEnd), bits.Slice(0, bitEnd));

                    ByteLength += byteEnd;

                    bits = bits.Slice(bitEnd);
                }
                
                if (bits.Length > 0)
                {
                    // Fill byte buffer with last unfilled
                    BitOverhead.Fill(bits);
                }
            }

            return this;
        }

        public IBufferBuilder AppendByte(byte byteValue)
        {
            if (BitOverhead.Length > 0)
            {
                // Fill current bit buffer
                int available = BitOverhead.AvailableLength;

                // Convert byte to bit array
                Span<bool> bits = BitUtility.ToBits(byteValue).AsSpan();

                // Fill byte buffer
                BitOverhead.Fill(bits.Slice(0, available));

                // Commit to memory and reset
                CommitBitBuffer();

                // Fill new bit buffer
                BitOverhead.Fill(bits.Slice(available));
            }
            else
            {
                EnsureAdditionalBytes(1);
                // Raw add to memory
                Memory.Span[ByteLength] = byteValue;
                ByteLength++;
            }

            return this;
        }

        public IBufferBuilder AppendBytes(ReadOnlySpan<byte> bytes)
        {
            if (BitOverhead.Length == 0)
            {
                EnsureAdditionalBytes(bytes.Length);
                // Raw Span copy to memory
                bytes.CopyTo(Memory.Span.Slice(ByteLength, bytes.Length));
                ByteLength += bytes.Length;
            }
            else
            {
                // Convert Bytes to Bits streamed in batchsize = DefaultBatchSize
                int offset = 0;
                while (offset < bytes.Length)
                {
                    int remainingBytes = bytes.Length - offset;
                    int bufferLength = Math.Min(DefaultBatchSize, remainingBytes);

                    // Append batch bits, but length = 0 because its added later
                    AppendBits(BitUtility.ToBits(bytes.Slice(offset, bufferLength)));
                    offset += bufferLength;
                }
            }

            return this;
        }

        public IBufferBuilder AppendStruct(bool value) => AppendBit(value);
        public IBufferBuilder AppendStruct(byte value) => AppendByte(value);
        public IBufferBuilder AppendStruct<T>(T value) where T : struct
        {
#if NETCOREAPP3_1_OR_GREATER
            return AppendStructs(MemoryMarshal.CreateReadOnlySpan(ref value, 1));
#else
            return AppendStructs<T>(new T[] { value }.AsSpan());
#endif
        }

        public IBufferBuilder AppendStructs(ReadOnlySpan<bool> values) => AppendBits(values);
        public IBufferBuilder AppendStructs(ReadOnlySpan<byte> values) => AppendBytes(values);
        public IBufferBuilder AppendStructs<T>(ReadOnlySpan<T> values) where T : struct
        {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(values);
            return AppendBytes(bytes);
        }

        public IBufferBuilder AppendArrow(ArrowBuffer buffer) => AppendBytes(buffer.Span);

        internal IBufferBuilder ReserveBytes(int numBytes)
        {
            EnsureAdditionalBytes(numBytes);
            return this;
        }

        internal IBufferBuilder ResizeBytes(int numBytes)
        {
            EnsureBytes(numBytes);
            ByteLength = numBytes;
            return this;
        }

        public IBufferBuilder Clear()
        {
            Memory.Span.Fill(default);
            ByteLength = 0;
            return this;
        }

        public ArrowBuffer Build(MemoryAllocator allocator = default) => Build(64, allocator);

        public ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default)
        {
            if (BitOverhead.Length > 0)
                CommitBitBuffer(true);

            int bufferLength = checked((int)BitUtility
                .RoundUpToMultiplePowerOfTwo(ByteLength, byteSize));

            MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
            IMemoryOwner<byte> memoryOwner = memoryAllocator.Allocate(bufferLength);
            Memory.Slice(0, Memory.Length).CopyTo(memoryOwner.Memory);

            return new ArrowBuffer(memoryOwner);
        }

        private void EnsureAdditionalBits(int numBits) => EnsureBytes(checked(Memory.Length + numBits / 8));
         
        private void EnsureAdditionalBytes(int numBytes) => EnsureBytes(checked(Memory.Length + numBytes));

        public void EnsureBytes(int numBytes)
        {
            if (numBytes > Memory.Length)
            {
                Reallocate(numBytes);
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
        public int ValueLength => (Memory.Span.Length * 8 + BitOverhead.Length) / ValueBitSize;

        public ValueBufferBuilder(int valueBitSize, int capacity = 64) : base(capacity)
        {
            ValueBitSize = valueBitSize;
        }

        public IValueBufferBuilder Ensure(int capacity)
        {
            EnsureBytes((capacity * ValueBitSize + 7) / 8);
            return this;
        }

        public IValueBufferBuilder Reserve(int capacity)
        {
            ReserveBytes((capacity * ValueBitSize + 7) / 8);
            return this;
        }

        public IValueBufferBuilder Resize(int capacity)
        {
            ResizeBytes((capacity * ValueBitSize + 7) / 8);
            return this;
        }
    }

    public class ValueBufferBuilder<T> : ValueBufferBuilder, IValueBufferBuilder<T> where T : struct
    {
        private static int GetBitSizeOf() => typeof(T) == typeof(bool) ? 1 : Unsafe.SizeOf<T>() * 8;

        public ValueBufferBuilder(int capacity = 64) : base(GetBitSizeOf(), capacity)
        {
        }

        public IValueBufferBuilder<T> AppendValue(T value)
        {
            AppendStruct(value);
            return this;
        }
        public IValueBufferBuilder<T> AppendValue(T? value) => AppendValue(value.GetValueOrDefault());
        public IValueBufferBuilder<T> AppendValues(ReadOnlySpan<T> values)
        {
            AppendStructs(values);
            return this;
        }
        public IValueBufferBuilder<T> AppendValues(ReadOnlySpan<T?> values)
        {
            Span<T> destination = new T[values.Length];

            // Transform the source ReadOnlySpan<T?> into the destination ReadOnlySpan<T>, filling any null values with default(T)
            for (int i = 0; i < values.Length; i++)
            {
                T? value = values[i];
                destination[i] = value ?? default;
            }

            return AppendValues(destination);
        }
    }
}

