using System;

namespace Apache.Arrow.Ipc.Compression
{
    /// <summary>
    /// Default compression provider that uses reflection to use common compression packages.
    /// </summary>
    internal sealed class DefaultCompressionProvider : ICompressionProvider
    {
        public IDecompressor GetDecompressor(CompressionType compressionType)
        {
            return compressionType switch
            {
                CompressionType.Lz4Frame => new Lz4Decompressor(),
                CompressionType.Zstd => new ZstdDecompressor(),
                _ => throw new NotImplementedException($"Compression type {compressionType} is not supported")
            };
        }
    }
}
