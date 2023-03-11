// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Runtime.InteropServices;

// Adapted from:
// https://github.com/G-Research/ParquetSharp/blob/467d99298fb5a5b9d5935b9c8dbde95e63954dd3/csharp/StringUtil.cs

namespace Apache.Arrow.C
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
