using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal class LongMemoryOwner<T>: IMemoryOwner<T>
    {
        private readonly LongMemory<T> _memory;

        public LongMemoryOwner(long length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");

            _memory = new LongMemory<T>(length);
        }

        public Memory<T> Memory
        {
            get { return new Memory<T>(); }
        }

        public void Dispose()
        {

        }
    }
}
