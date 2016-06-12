using System.Collections.Generic;
using System.Threading.Tasks;
using Ve.Messaging.Model;

namespace Ve.Messaging.Publisher
{
    public interface IMessagePublisher
    {
        Task SendAsync(Message message);
        Task SendBatchAsync(IEnumerable<Message> messages);
    }
}