using Ve.Metrics.StatsDClient.Abstract;
using Ve.Messaging.Azure.ServiceBus.Publisher.Wrapper;

namespace Ve.Messaging.Azure.ServiceBus.Publisher
{
    public interface ITopicClientCreator
    {
        ITopicClientWrapper CreateTopicClient(
            TopicConfiguration config,
            IVeStatsDClient statsDClient);
    }
}