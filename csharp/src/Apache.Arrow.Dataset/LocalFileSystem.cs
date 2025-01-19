namespace Apache.Arrow.Dataset;

public class LocalFileSystem : FileSystem
{
    public LocalFileSystem() : this(Apache.Arrow.GLibBindings.LocalFileSystem.New(null))
    {
    }

    public LocalFileSystem(LocalFileSystemOptions options)
        : this(Apache.Arrow.GLibBindings.LocalFileSystem.New(options.GObj))
    {
    }

    internal LocalFileSystem(Apache.Arrow.GLibBindings.LocalFileSystem gObj)
        : base(Apache.Arrow.GLibBindings.LocalFileSystem.New(null))
    {
    }

    static LocalFileSystem()
    {
        GLibBindings.Module.Initialize();
    }
}
