using System.Collections.Generic;
using Ve.Messaging.Model;

namespace Ve.Messaging.Consumer
{
    public interface IMessageConsumer
    {
        List<Message> RetrieveMessages(int messageAmount, int timeout);
    }
}