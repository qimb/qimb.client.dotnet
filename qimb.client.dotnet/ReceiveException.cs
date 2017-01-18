using System;

namespace qimb.client.dotnet
{
    public class ReceiveException : Exception
    {
        public ReceiveException(string message)
            : base(message)
        { }

        public ReceiveException(string message, Exception innerException)
            : base(message, innerException)
        { }
    }
}