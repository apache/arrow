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

using System.Runtime.InteropServices;
using System;
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class HashJoinNodeOptions : ExecNodeOptions
    {
        private readonly unsafe GArrowHashJoinNodeOptions* _optionsPtr;
        private readonly IntPtr _leftKeysPtr;
        private readonly IntPtr _rightKeysPtr;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowHashJoinNodeOptions* d_garrow_hash_join_node_options_new(GArrowJoinType type, IntPtr left_keys, uint n_left_keys, IntPtr right_keys, uint n_right_keys, out GError** error);
        private static d_garrow_hash_join_node_options_new garrow_hash_join_node_options_new = FuncLoader.LoadFunction<d_garrow_hash_join_node_options_new>("garrow_hash_join_node_options_new");

        internal unsafe GArrowHashJoinNodeOptions* Handle => _optionsPtr;

        public unsafe HashJoinNodeOptions(GArrowJoinType joinType, string[] leftKeys, string[] rightKeys)
        {
            _leftKeysPtr = GLib.Marshaller.StringArrayToStrvPtr(leftKeys);
            _rightKeysPtr = GLib.Marshaller.StringArrayToStrvPtr(leftKeys);

            _optionsPtr = garrow_hash_join_node_options_new(joinType, _leftKeysPtr, (uint)leftKeys.Length, _rightKeysPtr, (uint)rightKeys.Length, out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }

        ~HashJoinNodeOptions()
        {
            GLib.Marshaller.StrFreeV(_leftKeysPtr);
            GLib.Marshaller.StrFreeV(_rightKeysPtr);
        }
    }
}
