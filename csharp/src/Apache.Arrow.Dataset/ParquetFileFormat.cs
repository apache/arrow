namespace Apache.Arrow.Dataset;

/// <summary>
/// Parquet file format
/// </summary>
public class ParquetFileFormat : FileFormat
{
    public ParquetFileFormat()
        : base(GLibBindings.ParquetFileFormat.New())
    {
    }

    static ParquetFileFormat()
    {
        GLibBindings.Module.Initialize();
    }
}
