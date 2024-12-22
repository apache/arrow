using GObject;

namespace Apache.Arrow.Dataset.GLibBindings;

public partial class FileSystemDatasetWriteOptions
{
    // The FileSystem property must be manually defined as it isn't auto-generated, a warning is logged like:
    // Did not generate property 'FileSystemDatasetWriteOptions.file-system': The type ?? / Arrow.FileSystem has not been resolved.
    // TODO: Fix this in gir.core.

    public static readonly Property<Apache.Arrow.GLibBindings.FileSystem?, FileSystemDatasetWriteOptions> FileSystemPropertyDefinition = new (
        unmanagedName: "file-system",
        managedName: nameof(FileSystem)
    );

    public Apache.Arrow.GLibBindings.FileSystem? FileSystem
    {
        get => FileSystemPropertyDefinition.Get(this);
        set => FileSystemPropertyDefinition.Set(this, value);
    }
}
