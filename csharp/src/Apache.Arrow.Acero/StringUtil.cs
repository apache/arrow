using System.Runtime.InteropServices;

namespace Apache.Arrow.Acero
{
    internal static class StringUtil
    {
        public static unsafe byte* ToCStringUtf8(string str)
        {
            var utf8 = System.Text.Encoding.UTF8;
            int byteCount = utf8.GetByteCount(str);
            byte* byteArray = (byte*)Marshal.AllocHGlobal(byteCount + 1);

            fixed (char* chars = str)
            {
                utf8.GetBytes(chars, str.Length, byteArray, byteCount);
            }

            // Need to make sure it is null-terminated.
            byteArray[byteCount] = 0;

            return byteArray;
        }

        public static unsafe string PtrToStringUtf8(byte* ptr)
        {
#if NETSTANDARD2_1_OR_GREATER
            return Marshal.PtrToStringUTF8(ptr);
#else
            if (ptr == null)
            {
                return null;
            }

            int length;
            for (length = 0; ptr[length] != '\0'; ++length)
            {
            }

            return System.Text.Encoding.UTF8.GetString(ptr, length);
#endif
        }
    }
}
