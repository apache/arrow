using System;

namespace Apache.Arrow.Dataset;

public sealed class FileWriteOptions : IDisposable
{
    internal FileWriteOptions(GLibBindings.FileWriteOptions gLibWriteOptions)
    {
        GObj = gLibWriteOptions;
    }

    public void Dispose()
    {
        GObj.Dispose();
    }

    internal readonly GLibBindings.FileWriteOptions GObj;
}
