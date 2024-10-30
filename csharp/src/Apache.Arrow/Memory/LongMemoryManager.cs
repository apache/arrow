using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Apache.Arrow.Memory
{
    internal class LongMemoryManager: MemoryManager<byte>
    {
        private readonly LongMemory<byte> _memory;

        public LongMemoryManager(long length)
        {
            _memory = new LongMemory<byte>(length);
        }

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            
        }
        public override void Unpin()
        {

        }
        public override Memory<byte> Memory
        {
            
        }
        public override Span<byte> GetSpan()
        {
            return _memory.Span;
        }

        public override void Dispose(bool Disposing)
        {

        }

    }
}
