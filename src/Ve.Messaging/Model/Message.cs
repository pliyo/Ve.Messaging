using System.Collections.Generic;

namespace Ve.Messaging.Model
{
    public class Message
    {
        public object Content { get; set; }
        public string Label { get; set; }
        public string SessionId { get; set; }
        public Dictionary<string, object> Properties { get; set; } 
    }
}
