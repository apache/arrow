namespace Apache.Arrow.Flight.Sql;

public class Transaction(string? transactionId)
{
    public string? TransactionId { get; } = transactionId;

    public bool IsValid() => !string.IsNullOrEmpty(TransactionId);
}
