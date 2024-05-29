using System;
using Apache.Arrow.C;

namespace Apache.Arrow.Dataset;

public class DirectoryPartitioning : Partitioning
{
    public DirectoryPartitioning(Schema partitioningSchema)
        : this(GLibBindings.DirectoryPartitioning.New(ExportSchema(partitioningSchema), null, null))
    {
    }

    internal DirectoryPartitioning(GLibBindings.DirectoryPartitioning partitioning) : base(partitioning)
    {
    }

    private static unsafe Apache.Arrow.GLibBindings.Schema ExportSchema(Schema schema)
    {
        var cSchema = new CArrowSchema();
        CArrowSchemaExporter.ExportSchema(schema, &cSchema);
        return Apache.Arrow.GLibBindings.Schema.Import(new IntPtr(&cSchema))!;
    }

    static DirectoryPartitioning()
    {
        GLibBindings.Module.Initialize();
    }
}
