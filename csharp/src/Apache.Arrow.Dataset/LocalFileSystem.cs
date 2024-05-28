namespace Apache.Arrow.Dataset;

public class LocalFileSystem : FileSystem
{
    public LocalFileSystem() : base(Apache.Arrow.GLibBindings.LocalFileSystem.New(null))
    {
    }

    public LocalFileSystem(LocalFileSystemOptions options)
        : base(Apache.Arrow.GLibBindings.LocalFileSystem.New(options.GObj))
    {
    }

    static LocalFileSystem()
    {
        GLibBindings.Module.Initialize();
    }
}
