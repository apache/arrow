using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using static Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public class UnionNode : ExecNode
    {
        private unsafe CLib.GArrowExecuteNode* _nodePtr;

        public unsafe UnionNode(ExecPlan plan, List<ExecNode> nodes)
        {
            var factoryName = "union";
            var factoryNamePtr = (IntPtr)StringUtil.ToCStringUtf8(factoryName);

            var lhs = nodes[0];
            var rhs = nodes[1];

            var list = new GLib.List(IntPtr.Zero);
            list.Append((IntPtr)lhs.GetPtr());
            list.Append((IntPtr)rhs.GetPtr());

            // todo: using this ptr causes the following error
            // g_object_new_valist: invalid unclassed object pointer for value type 'GArrowExecuteNodeOptions'
            IntPtr optionsPtr = Marshal.AllocHGlobal(Marshal.SizeOf<GArrowExecuteNodeOptions>());
            Marshal.StructureToPtr(new GArrowExecuteNodeOptions(), optionsPtr, false);

            // for now just use the sink options
            var optionsPtr2 = garrow_sink_node_options_new();

            GError** error;

            _nodePtr = garrow_execute_plan_build_node(plan.GetPtr(), factoryNamePtr, list.Handle, (IntPtr)optionsPtr2, out error);

            ExceptionUtil.ThrowOnError(error);
        }

        public override unsafe GArrowExecuteNode* GetPtr() => _nodePtr;
    }
}
