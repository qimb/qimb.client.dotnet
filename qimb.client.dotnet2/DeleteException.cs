using System;

namespace qimb.client.dotnet
{
    public class DeleteException : Exception
    {
        public DeleteException(string message)
            : base(message)
        { }
    }
}