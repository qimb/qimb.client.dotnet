using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace qimb.client.dotnet
{
    public class SubscribeException : Exception
    {
        public SubscribeException(string message)
            : base(message)
        { }
    }
}
