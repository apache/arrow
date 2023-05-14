using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Apache.Arrow.Memory;
using Apache.Arrow.Reflection;
using Apache.Arrow.Types;

namespace Apache.Arrow.Builder
{
    public class BufferBuilder : IBufferBuilder
    {
        private const int DefaultCapacity = 64;
        public int ByteLength { get; internal set; }
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

            var span = Memory.Span;

            if (BitOffset == 0)
            {
                int wholeBytes = count / 8;

                if (wholeBytes > 0)
                {
                    EnsureAdditionalBytes(wholeBytes + 1);

                    var fill = (byte)0xFF;
                    span.Slice(ByteLength, wholeBytes).Fill(fill);
                    ByteLength += wholeBytes;
                }

                // Write remaining bits
                int trailing = count - (wholeBytes * 8);
                for (int i = 0; i < trailing; i++)
                    UncheckedAppendBit(value);
            }
            else
            {
                int available = 8 - BitOffset;

                if (count < available)
                {
                    for (int i = 0; i < count; i++)
                        UncheckedAppendBit(value);
                    return this;
                }

                // Write all remaining bits
                for (int i = 0; i < available; i++)
                    UncheckedAppendBit(value);

                // Commit to memory
                BitOffset = 0;
                ByteLength++;
                // Ensure current and another byte for next append
                EnsureBytes(ByteLength + 1);

                int trailing = count - available;
                AppendBits(value, trailing);
            }

            return this;
        }

        // Write false bool values = AppendBits(false, count)
        public IBufferBuilder AppendEmptyBits(int count)
        {
            if (BitOffset == 0)
            {
                int wholeBytes = count / 8;

                if (wholeBytes > 0)
                {
                    EnsureAdditionalBytes(wholeBytes + 1);
                    ByteLength += wholeBytes;
                }

                // Write remaining bits
                BitOffset = count - (wholeBytes * 8);
            }
            else
            {
                int available = 8 - BitOffset;

                if (count < available)
                {
                    BitOffset += count;
                    return this;
                }

                // Commit to memory
                BitOffset = 0;
                ByteLength++;
                // Ensure current and another byte for next append
                EnsureBytes(ByteLength + 1);

                int trailing = count - available;
                AppendEmptyBits(trailing);
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
                    if (value.Length > fixedSize)
                        value.AsSpan().Slice(0, fixedSize).CopyTo(Memory.Span.Slice(offset, fixedSize));
                    else
                        value.CopyTo(Memory.Span.Slice(offset, fixedSize));
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

        public IBufferBuilder AppendBytes(ReadOnlySpan<byte> value, int count)
        {
            if (BitOffset != 0)
                throw new NotSupportedException("Cannot append repeated bytes while current buffer BitOffset != 0");
            int total = count * value.Length;
            EnsureAdditionalBytes(total);
            int offset = ByteLength;
            
            for (int i = 0; i < count; i++)
            {
                value.CopyTo(Memory.Span.Slice(offset, value.Length));
                offset += value.Length;
            }

            ByteLength += total;
            return this;
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

        public async Task<ArrowBuffer> BuildAsync(MemoryAllocator allocator = default)
            => await BuildAsync(64, allocator);

        public ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default)
        {
            var byteLength = BitOffset > 0 ? ByteLength + 1 : ByteLength;

            int bufferLength = checked((int)BitUtility.RoundUpToMultiplePowerOfTwo(byteLength, byteSize));

            MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
            IMemoryOwner<byte> memoryOwner = memoryAllocator.Allocate(bufferLength);
            Memory.Slice(0, byteLength).CopyTo(memoryOwner.Memory);

            return new ArrowBuffer(memoryOwner);
        }

        public async Task<ArrowBuffer> BuildAsync(int byteSize, MemoryAllocator allocator = default)
        {
            var byteLength = BitOffset > 0 ? ByteLength + 1 : ByteLength;

            int bufferLength = checked((int)BitUtility.RoundUpToMultiplePowerOfTwo(byteLength, byteSize));

            MemoryAllocator memoryAllocator = allocator ?? MemoryAllocator.Default.Value;
            IMemoryOwner<byte> memoryOwner = memoryAllocator.Allocate(bufferLength);
            Memory.Slice(0, byteLength).CopyTo(memoryOwner.Memory);

            return new ArrowBuffer(memoryOwner);
        }

        internal void EnsureAdditionalBytes(int numBytes) => EnsureBytes(checked(ByteLength + numBytes));

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

    public class TypedBufferBuilder : BufferBuilder, ITypedBufferBuilder
    {
        public bool IsVariableLength { get; }
        public int ValueBitSize { get; }
        public int ValueByteSize { get; }
        public int ValueLength => (ByteLength * 8 + BitOffset) / ValueBitSize;
        private int MinimumBitSize => IsVariableLength ? 8 : ValueBitSize;

        public TypedBufferBuilder(int valueBitSize, int capacity = 32) : base(capacity * (valueBitSize + 7) / 8)
        {
            IsVariableLength = valueBitSize < 0;
            ValueBitSize = valueBitSize;
            ValueByteSize = valueBitSize / 8;
        }

        public ITypedBufferBuilder Ensure(int capacity)
        {
            EnsureBytes((capacity * MinimumBitSize + 7) / 8);
            return this;
        }

        public ITypedBufferBuilder Reserve(int additionnalCapacity)
        {
            ReserveAdditionalBytes((additionnalCapacity * MinimumBitSize + 7) / 8);
            return this;
        }

        public ITypedBufferBuilder Resize(int capacity)
        {
            ResizeBytes((capacity * MinimumBitSize + 7) / 8);
            return this;
        }
    }

    public class TypedBufferBuilder<T> : TypedBufferBuilder, ITypedBufferBuilder<T> where T : struct
    {
        public TypedBufferBuilder(int capacity = 32) : this((TypeReflection<T>.ArrowType as FixedWidthType).BitWidth, capacity)
        {
        }

        public TypedBufferBuilder(int bitWidth, int capacity = 32) : base(bitWidth, capacity)
        {
        }

        public ITypedBufferBuilder<T> AppendValue(T value)
            => AppendValues(TypeReflection.CreateReadOnlySpan(ref value));

        public ITypedBufferBuilder<T> AppendValue(bool value)
        {
            AppendBit(value);
            return this;
        }

        public ITypedBufferBuilder<T> AppendValues(ReadOnlySpan<T> values)
        {
            ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(values);
            AppendBytes(bytes);
            return this;
        }

        public ITypedBufferBuilder<T> AppendValues(ReadOnlySpan<bool> values)
        {
            AppendBits(values);
            return this;
        }

        public ITypedBufferBuilder<T> AppendValues(T value, int count)
        {
            AppendBytes(TypeReflection.AsBytes(ref value), count);
            return this;
        }

        public ITypedBufferBuilder<T> AppendValues(bool value, int count)
        {
            AppendBits(value, count);
            return this;
        }

        public ITypedBufferBuilder<T> AppendValues(
            ICollection<T?> values, Span<bool> validity, out int nullCount
            )
        {
            int i = 0;
            int offset = 0;
            int fixedSize = ValueByteSize;
            int _nullCount = 0;
            EnsureAdditionalBytes(values.Count * fixedSize);

            foreach (T? value in values)
            {
                if (value.HasValue)
                {
                    T real = value.Value;
                    TypeReflection.AsBytes(ref real).CopyTo(Memory.Span.Slice(offset, fixedSize));
                    validity[i] = true;
                }
                else
                    _nullCount++;
                offset += fixedSize;
                i++;
            }

            ByteLength += offset;

            nullCount = _nullCount;
            return this;
        }

        public ITypedBufferBuilder<T> AppendValues(
            ICollection<bool?> values, Span<bool> validity, out int nullCount
            )
        {
            int i = 0;
            int _nullCount = 0;
            int length = values.Count;
            bool allTrue = true;
            bool allFalse = true;
            Span<bool> bits = new bool[length];
            EnsureAdditionalBytes((length + 7) / 8);

            foreach (bool? value in values)
            {
                if (value.HasValue)
                {
                    bool _value = value.Value;

                    if (_value)
                    {
                        allFalse = false;
                        bits[i] = _value;
                    }
                    else
                        allTrue = false;

                    validity[i] = true;
                }
                else
                    _nullCount++;
                i++;
            }

            nullCount = _nullCount;

            if (allTrue)
                AppendBits(true, length);
            if (allFalse)
                AppendBits(false, length);
            else
                AppendBits(bits);
            return this;
        }
    }
}

