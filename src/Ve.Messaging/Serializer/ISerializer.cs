using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ve.Messaging.Serializer
{
    public interface ISerializer
    {
        T ParseMessage<T>(Stream content);
        Stream Serialize<T>(T content);
    }
}
