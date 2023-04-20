using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace Apache.Arrow.Util
{
    public class HashUtil
    {
        internal enum Seed : uint
        {
            String
        }

#if NETCOREAPP2_0_OR_GREATER
        internal static int Hash32(string str, Encoding encoding = null)
        {
            encoding = encoding ?? Encoding.UTF8;
            byte[] compressed = str.Length > 128 ? Compress(encoding.GetBytes(str)) : encoding.GetBytes(str);
            ReadOnlySpan<byte> span = compressed.AsSpan();
            return Hash32(span, Seed.String);
        }

        internal static int Hash32(ReadOnlySpan<byte> data, Seed seed)
        {
            const uint c1 = 0xcc9e2d51;
            const uint c2 = 0x1b873593;
            const int r1 = 15;
            const int r2 = 13;
            const uint m = 5;
            const uint n = 0xe6546b64;

            uint hash = (uint)seed;
            int remainingBytes = data.Length & 3;
            int byteCount = data.Length - remainingBytes;

            for (int i = 0; i < byteCount; i += 4)
            {
                uint k = BitConverter.ToUInt32(data.Slice(i, 4));
                k *= c1;
                k = (k << r1) | (k >> (32 - r1));
                k *= c2;

                hash ^= k;
                hash = ((hash << r2) | (hash >> (32 - r2))) * m + n;
            }

            if (remainingBytes > 0)
            {
                uint k = 0;
                for (int i = byteCount; i < data.Length; i++)
                {
                    k |= (uint)data[i] << ((i - byteCount) * 8);
                }

                k *= c1;
                k = (k << r1) | (k >> (32 - r1));
                k *= c2;

                hash ^= k;
            }

            hash ^= (uint)data.Length;
            hash ^= hash >> 16;
            hash *= 0x85ebca6b;
            hash ^= hash >> 13;
            hash *= 0xc2b2ae35;
            hash ^= hash >> 16;

            return (int)hash;
        }
#else
        internal static int Hash32(string str)
        {
            const uint FNV_prime = 16777619;
            const uint FNV_offset_basis = 2166136261;

            uint hash = FNV_offset_basis;
            ReadOnlySpan<char> span = str.AsSpan();

            for (int i = 0; i < span.Length; i++)
            {
                hash ^= span[i];
                hash *= FNV_prime;
            }

            return (int)hash;
        }
#endif
        internal static byte[] Compress(byte[] data)
        {
            using (MemoryStream compressedStream = new MemoryStream())
            {
                using (DeflateStream compressionStream = new DeflateStream(compressedStream, CompressionMode.Compress))
                {
                    compressionStream.Write(data, 0, data.Length);
                }
                return compressedStream.ToArray();
            }
        }
        public static int CombineHash32(int h1, int h2) => Tuple.Create(h1, h2).GetHashCode();
        public static int CombineHash32(int h1, int h2, int h3) => Tuple.Create(h1, h2, h3).GetHashCode();

        public static int CombineHash32(params int[] hashCodes)
        {
            int hash = 0;

            foreach (int hashCode in hashCodes)
            {
                hash = CombineHash32(hash, hashCode);
            }

            return hash;
        }        
    }
}
