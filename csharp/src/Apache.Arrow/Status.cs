namespace Apache.Arrow
{
    public enum StatusCode
    {
        OK = 0,
        OutOfMemory = 1,
        KeyError = 2,
        TypeError = 3,
        Invalid = 4,
        IOError = 5,
        CapacityError = 6,
        IndexError = 7,
        Cancelled = 8,
        UnknownError = 9,
        NotImplemented = 10,
        SerializationError = 11,
        RError = 13,
        // Gandiva range of errors
        CodeGenError = 40,
        ExpressionValidationError = 41,
        ExecutionError = 42,
        // Continue generic codes.
        AlreadyExists = 45
    }

    public class Status
    {
        public static readonly Status OK = new(StatusCode.OK);

        public readonly StatusCode Code;
        public readonly string Message;

        public Status(StatusCode code, string msg = null)
        {
            Code = code;
            Message = msg;
        }
    }
}
