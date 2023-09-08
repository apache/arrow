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
using Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class ExecPlan
    {
        private readonly unsafe GArrowExecutePlan* _planPtr;

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate GArrowExecutePlan* d_garrow_execute_plan_new(out GError** error);
        private static d_garrow_execute_plan_new garrow_execute_plan_new = FuncLoader.LoadFunction<d_garrow_execute_plan_new>("garrow_execute_plan_new");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate bool d_garrow_execute_plan_validate(GArrowExecutePlan* plan, out GError** error);
        private static d_garrow_execute_plan_validate garrow_execute_plan_validate = FuncLoader.LoadFunction<d_garrow_execute_plan_validate>("garrow_execute_plan_validate");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate bool d_garrow_execute_plan_start(GArrowExecutePlan* plan);
        private static d_garrow_execute_plan_start garrow_execute_plan_start = FuncLoader.LoadFunction<d_garrow_execute_plan_start>("garrow_execute_plan_start");

        [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
        private unsafe delegate bool d_garrow_execute_plan_wait(GArrowExecutePlan* plan);
        private static d_garrow_execute_plan_wait garrow_execute_plan_wait = FuncLoader.LoadFunction<d_garrow_execute_plan_wait>("garrow_execute_plan_wait");

        public unsafe GArrowExecutePlan* Handle => _planPtr;

        public unsafe ExecPlan()
        {
            _planPtr = garrow_execute_plan_new(out GError** error);

            ExceptionUtil.ThrowOnError(error);
        }

        public unsafe bool Validate()
        {
            bool valid = garrow_execute_plan_validate(_planPtr, out GError** error);

            ExceptionUtil.ThrowOnError(error);

            return valid;
        }

        public unsafe void StartProducing()
        {
            garrow_execute_plan_start(_planPtr);
        }

        public unsafe void Wait()
        {
            garrow_execute_plan_wait(_planPtr);
        }
    }
}
