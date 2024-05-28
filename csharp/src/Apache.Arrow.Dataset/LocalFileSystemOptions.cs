namespace Apache.Arrow.Dataset;

public class LocalFileSystemOptions
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

    internal readonly Apache.Arrow.GLibBindings.LocalFileSystemOptions GObj =
        Apache.Arrow.GLibBindings.LocalFileSystemOptions.New();
}
