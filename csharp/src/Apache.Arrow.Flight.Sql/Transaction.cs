using Google.Protobuf;

namespace Apache.Arrow.Flight.Sql;

public class Transaction
{
    private static readonly ByteString TransactionIdDefaultValue = ByteString.Empty;
    private ByteString? _transactionId;

    public ByteString TransactionId
    {
        get => _transactionId ?? TransactionIdDefaultValue;
        set => _transactionId = ProtoPreconditions.CheckNotNull(value, nameof(value));
    }

    public static readonly Transaction NoTransaction = new(TransactionIdDefaultValue);

    public Transaction(ByteString transactionId)
    {
        TransactionId = transactionId;
    }

    public Transaction(string transactionId)
    {
        _transactionId = ByteString.CopyFromUtf8(transactionId);
    }

    public bool IsValid() => TransactionId.Length > 0;
    public void ResetTransaction()
    {
        _transactionId = TransactionIdDefaultValue;
    }
}
