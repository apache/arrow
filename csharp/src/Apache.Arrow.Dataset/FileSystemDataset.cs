namespace Apache.Arrow.Dataset;

/// <summary>
/// Dataset that reads from data files stored in a file system
/// </summary>
public class FileSystemDataset : DatasetBase
{
    internal FileSystemDataset(GLibBindings.FileSystemDataset glibFileSystemDataset)
        : base(glibFileSystemDataset)
    {
    }
}
