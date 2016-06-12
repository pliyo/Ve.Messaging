using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace Ve.Messaging.Azure.ServiceBus.Publisher.Wrapper
{
    public interface ITopicClientWrapper
    {
        Task SendAsync(BrokeredMessage message);
    }
}
