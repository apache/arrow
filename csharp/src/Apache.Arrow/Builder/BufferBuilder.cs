using System;
using System.Buffers;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Builder
{
    public class BufferBuilder : IBufferBuilder
    {
        public class BitBuffer
        {
            private const int DefaultCapacity = 8;

            private bool[] _bits;
            public Span<bool> Bits => _bits.AsSpan().Slice(0, Length);

            public int Length { get; private set; }
            public int AvailableLength => Capacity - Length;

            public int Capacity;

            public bool IsFull => Length == Capacity;
            public byte ToByte => BitUtility.ToByte(_bits);

            public BitBuffer(int capacity = DefaultCapacity)
            {
                Capacity = capacity;
                _bits = new bool[capacity];
                Length = 0;
            }

            public void Append(bool bit) => _bits[Length++] = bit;
            public void Fill(ReadOnlySpan<bool> bits)
            {
                bits.CopyTo(_bits.AsSpan().Slice(Length, Capacity - bits.Length));
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
        private const int DefaultCapacity = 8;

        public int ValueBitSize { get; }
        public int ByteLength { get; private set; }

        public Memory<byte> Memory { get; private set; }
        private BitBuffer BitOverhead { get; }

        /// <summary>
        /// Creates an instance of the <see cref="BufferBuilder"/> class.
        /// </summary>
        /// <param name="valueBitSize">Number of bits of one value item.</param>
        /// <param name="capacity">Number of items of initial capacity to reserve.</param>
        public BufferBuilder(int valueBitSize, int capacity = DefaultCapacity)
        {
            ValueBitSize = valueBitSize;

            Memory = new byte[capacity * (valueBitSize + 7) / 8];
            BitOverhead = new BitBuffer();

            ByteLength = 0;
        }

        private void CommitBitBuffer()
        {
            if (BitOverhead.IsFull)
            {
                EnsureAdditionalBytes(1);
                Memory.Span[ByteLength] = BitOverhead.ToByte;
                BitOverhead.Reset();
                ByteLength++;
            }
        }

        public void AppendBit(bool bit)
        {
            BitOverhead.Append(bit);
            CommitBitBuffer();
        }

        public void AppendBits(ReadOnlySpan<bool> bits)
        {
            if (BitOverhead.Length != 0)
            {
                int available = BitOverhead.AvailableLength;

                // Fill byte buffer
                BitOverhead.Fill(bits.Slice(0, available));

                // Commit to memory and reset
                CommitBitBuffer();

                bits = bits.Slice(available);
            }

            ReadOnlySpan<byte> bytes = BitUtility.ToBytes(bits);
            
            int IsFullByte = bits.Length % 8;

            // Raw Span copy to memory
            if (IsFullByte == 0)
            {
                EnsureAdditionalBytes(bytes.Length);
                bytes.CopyTo(Memory.Span.Slice(ByteLength, bytes.Length));
                ByteLength += bytes.Length;
            }
            else
            {
                if (bytes.Length > 1)
                {
                    EnsureAdditionalBytes(bytes.Length - 1);
                    // copy full bytes
                    bytes
                        .Slice(0, bytes.Length - 1)
                        .CopyTo(Memory.Span.Slice(ByteLength, bytes.Length - 1));
                    ByteLength += bytes.Length;
                }

                // Fill byte buffer with last unfilled
                BitOverhead.Fill(BitUtility.ToBits(bytes[bytes.Length - 1]));
            }
        }

        public void AppendByte(byte byteValue)
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
        }

        public void AppendBytes(ReadOnlySpan<byte> bytes)
        {
            if (BitOverhead.Length == 0)
            {
                EnsureAdditionalBytes(bytes.Length);
                // Raw Span copy to memory
                bytes.CopyTo(Memory.Span.Slice(ByteLength, bytes.Length));
                ByteLength++;
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
        }

        public void ReserveBytes(int numBytes) => EnsureAdditionalBytes(numBytes);

        public void ResizeBytes(int numBytes)
        {
            EnsureBytes(numBytes);
            ByteLength = numBytes;
        }

        public void Clear()
        {
            Memory.Span.Fill(default);
            ByteLength = 0;
        }

        public ArrowBuffer Build(MemoryAllocator allocator = default) => Build(64, allocator);

        public ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default)
        {
            if (BitOverhead.Length > 0)
                CommitBitBuffer();

            int bufferLength = checked((int)BitUtility
                .RoundUpToMultiplePowerOfTwo(ByteLength, byteSize));

            MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
            IMemoryOwner<byte> memoryOwner = memoryAllocator.Allocate(bufferLength);
            Memory.Slice(0, Memory.Length).CopyTo(memoryOwner.Memory);

            return new ArrowBuffer(memoryOwner);
        }

        internal void EnsureAdditionalBits(int numBits) => EnsureBytes(checked(Memory.Length + numBits / 8));

        internal void EnsureAdditionalBytes(int numBytes) => EnsureBytes(checked(Memory.Length + numBytes));

        private void EnsureBytes(int numBytes)
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
}

