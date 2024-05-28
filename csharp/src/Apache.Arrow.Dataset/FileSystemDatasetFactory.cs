namespace Apache.Arrow.Dataset;

/// <summary>
/// Factory for building datasets that store data in a file system
/// </summary>
public class FileSystemDatasetFactory : DatasetFactory
{
    public FileSystemDatasetFactory(FileFormat format)
        : base(GLibBindings.FileSystemDatasetFactory.New(format.GObj))
    {
    }

    public FileSystemDatasetFactory SetFileSystem(FileSystem fileSystem)
    {
        GLibFileSystemDatasetFactory.SetFileSystem(fileSystem.GObj);
        return this;
    }

    public FileSystemDatasetFactory AddPath(string path)
    {
        GLibFileSystemDatasetFactory.AddPath(path);
        return this;
    }

    public override DatasetBase Finish()
    {
        return new FileSystemDataset(GLibFileSystemDatasetFactory.Finish(null)!);
    }

    private GLibBindings.FileSystemDatasetFactory GLibFileSystemDatasetFactory =>
        (GLibBindings.FileSystemDatasetFactory) GObj;
}
