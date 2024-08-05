namespace Apache.Arrow.Flight.Sql;

public class Savepoint
{
    public string SavepointId { get; private set; }

    public Savepoint(string savepointId)
    {
        SavepointId = savepointId;
    }

    public bool IsValid() => !string.IsNullOrEmpty(SavepointId);
}
