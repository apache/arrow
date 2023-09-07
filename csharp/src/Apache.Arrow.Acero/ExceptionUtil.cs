using System;
using static Apache.Arrow.Acero.CLib;

namespace Apache.Arrow.Acero
{
    public static class ExceptionUtil
    {
        public static unsafe void ThrowOnError(GError** error)
        {
            if ((IntPtr)error != IntPtr.Zero)
                throw new GLib.GException((IntPtr)error);
        }
    }
}
