using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace Ve.Messaging.Serializer
{
    public class SimpleSerializer : ISerializer
    {
        private IFormatter _formatter;
        public SimpleSerializer()
        {
            _formatter = new BinaryFormatter();
        }
        public T ParseMessage<T>(Stream content)
        {
            return (T)_formatter.Deserialize(content);
        }

        public Stream Serialize<T>(T content)
        {
            var stream = new MemoryStream();
            _formatter.Serialize(stream, content);
            return stream;
        }
    }
}
