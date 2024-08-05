namespace Apache.Arrow.Flight.Sql;

public class TableRef
{
    public string? Catalog { get; set; }
    public string DbSchema { get; set; } = null!;
    public string Table { get; set; } = null!;
}
