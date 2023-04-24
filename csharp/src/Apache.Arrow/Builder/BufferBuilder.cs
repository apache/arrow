using System;
using System.Buffers;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Builder
{
    public class BufferBuilder : IBufferBuilder
    {
        public class ByteBuffer
        {
            private bool[] _values;
            public Span<bool> Values => _values.AsSpan().Slice(0, Length);

            public int Length { get; private set; }
            public int AvailableLength => 8 - Length;

            public bool IsFull => Length == 8;
            public byte ToByte => BitUtility.ToByte(_values);

            public ByteBuffer()
            {
                _values = new bool[8];
                Length = 0;
            }

            public void Append(bool bit) => _values[Length++] = bit;
            public void Fill(ReadOnlySpan<bool> bits)
            {
                bits.CopyTo(_values.AsSpan().Slice(Length, 8 - bits.Length));
                Length += bits.Length;
            }

            public void Reset()
            {
                _values = new bool[8];
                Length = 0;
            }
        }

        private const int DefaultBatchSize = 65536; // 64 * 1024
        private const int DefaultCapacity = 8;

        public int ValueBitSize { get; }
        public int ValueByteSize { get; }

        public int Capacity => Memory.Length / ValueByteSize;

        public int ValueLength { get; private set; }

        public Memory<byte> Memory { get; private set; }
        private ByteBuffer BitBuffer { get; }
        public int UnsetBitLength => 8 - BitBuffer.Length;

        /// <summary>
        /// Creates an instance of the <see cref="BufferBuilder"/> class.
        /// </summary>
        /// <param name="valueBitSize">Number of bits of one value item.</param>
        /// <param name="capacity">Number of items of initial capacity to reserve.</param>
        public BufferBuilder(int valueBitSize, int capacity = DefaultCapacity)
        {
            ValueBitSize = valueBitSize;
            ValueByteSize = (valueBitSize + 7) / 8;

            Memory = new byte[capacity * ValueByteSize];
            BitBuffer = new ByteBuffer();
            ValueLength = 0;
        }

        private void CommitBitBuffer()
        {
            Memory.Span[Memory.Length] = BitBuffer.ToByte;
            BitBuffer.Reset();
        }

        public void AppendBit(bool bit)
        {
            EnsureAdditionalCapacity(1);
            BitBuffer.Append(bit);
            if (BitBuffer.IsFull)
                CommitBitBuffer();
        }

        public void AppendBits(ReadOnlySpan<bool> bits) => AppendBits(bits, bits.Length / ValueBitSize);

        public void AppendBits(ReadOnlySpan<bool> bits, int length)
        {
            EnsureAdditionalCapacity(length);

            if (BitBuffer.Length != 0)
            {
                int available = BitBuffer.AvailableLength;

                // Fill byte buffer
                BitBuffer.Fill(bits.Slice(0, available));

                // Commit to memory and reset
                CommitBitBuffer();

                bits = bits.Slice(available);
            }

            ReadOnlySpan<byte> bytes = BitUtility.ToBytes(bits);

            int IsFullByte = bits.Length % 8;

            // Raw Span copy to memory
            if (IsFullByte == 0)
                bytes.CopyTo(Memory.Span.Slice(ValueLength * ValueByteSize, bytes.Length * ValueByteSize));
            else
            {
                if (bytes.Length > 1)
                {
                    // copy full bytes
                    bytes
                        .Slice(0, bytes.Length - 1)
                        .CopyTo(Memory.Span.Slice(ValueLength * ValueByteSize, (bytes.Length - 1) * ValueByteSize));
                }

                // Fill byte buffer with last unfilled
                BitBuffer.Fill(BitUtility.ToBits(bytes[bytes.Length - 1]));
            }

            ValueLength += length;
        }

        public void AppendByte(byte bytes) => AppendByte(bytes, 8 / ValueBitSize);

        public void AppendByte(byte byteValue, int length)
        {
            EnsureAdditionalCapacity(length);

            if (BitBuffer.Length > 0)
            {
                // Fill current bit buffer
                int available = BitBuffer.AvailableLength;

                // Convert byte to bit array
                Span<bool> bits = BitUtility.ToBits(byteValue).AsSpan();

                // Fill byte buffer
                BitBuffer.Fill(bits.Slice(0, available));

                // Commit to memory and reset
                CommitBitBuffer();

                // Fill new bit buffer
                BitBuffer.Fill(bits.Slice(available));
            }
            else
            {
                // Raw add to memory
                Memory.Span[ValueLength * (ValueBitSize + 7) / 8] = byteValue;
            }

            ValueLength += length;
        }

        public void AppendBytes(ReadOnlySpan<byte> bytes) => AppendBytes(bytes, bytes.Length * 8 / ValueBitSize);

        public void AppendBytes(ReadOnlySpan<byte> bytes, int length)
        {
            EnsureAdditionalCapacity(length);

            if (BitBuffer.Length == 0)
            {
                // Raw Span copy to memory
                bytes.CopyTo(Memory.Span.Slice(ValueLength * ValueByteSize, bytes.Length * ValueByteSize));
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
                    AppendBits(BitUtility.ToBits(bytes.Slice(offset, bufferLength)), 0);
                    offset += bufferLength;
                }
            }

            ValueLength += length;
        }

        public void Reserve(int additionalCapacity) => EnsureAdditionalCapacity(additionalCapacity);

        public void Resize(int capacity)
        {
            EnsureCapacity(capacity);
            ValueLength = capacity;
        }

        public void Clear()
        {
            Memory.Span.Fill(default);
            ValueLength = 0;
        }

        public ArrowBuffer Build(MemoryAllocator allocator = default) => Build(64, allocator);

        public ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default)
        {
            if (BitBuffer.Length > 0)
                CommitBitBuffer();

            int bufferLength = checked((int)BitUtility
                .RoundUpToMultiplePowerOfTwo(BitUtility.ByteCount(ValueLength * ValueBitSize), byteSize));

            MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
            IMemoryOwner<byte> memoryOwner = memoryAllocator.Allocate(bufferLength);
            Memory.Slice(0, Memory.Length).CopyTo(memoryOwner.Memory);

            return new ArrowBuffer(memoryOwner);
        }

        internal void EnsureAdditionalCapacity(int addLength)
        {
            if (addLength > 0)
                EnsureCapacity(checked(ValueLength + addLength));
        }

        internal void EnsureCapacity(int length)
        {
            if (length > Capacity)
            {
                // TODO: specifiable growth strategy
                // Double the length of the in-memory array, or use the byte count of the capacity, whichever is
                // greater.
                int capacity = Math.Max(length * ValueByteSize, Memory.Length * 2);
                Reallocate(capacity);
            }
        }

        internal void Reallocate(int numBytes)
        {
            var memory = new Memory<byte>(new byte[numBytes]);
            Memory.CopyTo(memory);

            Memory = memory;
        }
    }
}

