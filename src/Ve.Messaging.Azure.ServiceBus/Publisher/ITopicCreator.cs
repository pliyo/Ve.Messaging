namespace Ve.Messaging.Azure.ServiceBus.Core
{
    public interface ITopicCreator
    {
        void SetTopic(string connectionString, string topicName, bool update = false);
    }
}