using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

namespace Ve.Messaging.Azure.ServiceBus.Core
{
    public class TopicCreator : ITopicCreator
    {
        public void SetTopic(string connectionString, string topicName, bool update = false)
        {
            var topicDescription = GetDefaultTopicDescription(topicName);
            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            CreateOrUpdateTopic(update, namespaceManager, topicDescription);
        }

        private static void CreateOrUpdateTopic(bool update,
                                                NamespaceManager namespaceManager,
                                                TopicDescription topicDescription)
        {
            if (!namespaceManager.TopicExists(topicDescription.Path))
            {
                namespaceManager.CreateTopic(topicDescription);
            }
            else if (update)
            {
                namespaceManager.UpdateTopic(topicDescription);
            }
        }

        private static TopicDescription GetDefaultTopicDescription(string topicName)
        {
            return new TopicDescription(topicName)
            {
                DefaultMessageTimeToLive = TimeSpan.FromHours(24),
                EnableBatchedOperations = true,
                MaxSizeInMegabytes = 1024*5,
                EnablePartitioning = true
            };
        }
    }
}