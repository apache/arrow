using System;

namespace Apache.Arrow.Dataset;

public sealed class LocalFileSystemOptions : IDisposable
{
    public bool UseMmap
    {
        get => GObj.UseMmap;
        set => GObj.UseMmap = value;
    }

    static LocalFileSystemOptions()
    {
        GLibBindings.Module.Initialize();
    }

    public void Dispose()
    {
        GObj.Dispose();
    }

    internal readonly Apache.Arrow.GLibBindings.LocalFileSystemOptions GObj =
        Apache.Arrow.GLibBindings.LocalFileSystemOptions.New();
}
