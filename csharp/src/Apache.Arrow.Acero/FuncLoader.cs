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

namespace Apache.Arrow.Acero
{
    internal class FuncLoader
    {
        const uint LOAD_LIBRARY_SEARCH_DEFAULT_DIRS = 0x00001000;

        private class Windows
        {
            [DllImport("kernel32.dll", CharSet = CharSet.Ansi, ExactSpelling = true, SetLastError = true)]
            public static extern IntPtr GetProcAddress(IntPtr hModule, string procName);

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            public static extern IntPtr LoadLibrary(string lpszLib);

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            public static extern IntPtr AddDllDirectory(string newDirectory);

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            public static extern bool SetDefaultDllDirectories(uint directoryFlags);
        }

        private class Linux
        {
            [DllImport("libdl.so.2")]
            public static extern IntPtr dlopen(string path, int flags);

            [DllImport("libdl.so.2")]
            public static extern IntPtr dlsym(IntPtr handle, string symbol);
        }

        private const int RTLD_LAZY = 0x0001;
        private const int RTLD_GLOBAL = 0x0100;

        public static IntPtr LoadLibrary(string libName)
        {
            if (OperatingSystem.IsWindows())
            {
                IntPtr ptr = Windows.LoadLibrary(libName);

                if (ptr == IntPtr.Zero)
                {
                    Windows.AddDllDirectory("C:\\msys64\\mingw64\\bin");
                    Windows.SetDefaultDllDirectories(LOAD_LIBRARY_SEARCH_DEFAULT_DIRS);

                    ptr = Windows.LoadLibrary(libName);
                }

                int error = Marshal.GetLastWin32Error();

                if (error > 0)
                    throw new DllNotFoundException($"Could not load {libName}");

                return ptr;
            }

            if (OperatingSystem.IsLinux())
                return Linux.dlopen(libName, RTLD_GLOBAL | RTLD_LAZY);

            return IntPtr.Zero;
        }

        public static IntPtr GetProcAddress(IntPtr library, string function)
        {
            if (OperatingSystem.IsWindows())
                return Windows.GetProcAddress(library, function);

            if (OperatingSystem.IsLinux())
                return Linux.dlsym(library, function);

            return IntPtr.Zero;
        }

        public static T LoadFunction<T>(IntPtr procAddress)
        {
            if (procAddress == IntPtr.Zero)
                return default(T);

            return Marshal.GetDelegateForFunctionPointer<T>(procAddress);
        }

        private static IntPtr NativeLibrary = GetNativeLibrary();

        private static IntPtr GetNativeLibrary()
        {
            if (OperatingSystem.IsWindows())
                return LoadLibrary("libarrow-glib-1300.dll");

            if (OperatingSystem.IsLinux())
                return LoadLibrary("libarrow-glib.so");

            return IntPtr.Zero;
        }

        public static T LoadFunction<T>(string function)
        {
            IntPtr procAddress = GetProcAddress(NativeLibrary, function);

            return LoadFunction<T>(procAddress);
        }
    }
}
