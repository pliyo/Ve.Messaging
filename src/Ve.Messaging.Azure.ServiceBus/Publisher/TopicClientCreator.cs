using System;
using System.Collections.Generic;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Ve.Metrics.StatsDClient.Abstract;
using Ve.Messaging.Azure.ServiceBus.Publisher.Wrapper;
using Ve.Messaging.Azure.ServiceBus.Publisher;

namespace Ve.Messaging.Azure.ServiceBus.Core
{
    public class TopicClientCreator : ITopicClientCreator
    {
        private readonly ITopicCreator _creator;
        private object _lock = new object();
        private static Dictionary<string, ITopicClientWrapper> _topics = new Dictionary<string, ITopicClientWrapper>();

        public TopicClientCreator(ITopicCreator creator)
        {
            _creator = creator;
        }

        public ITopicClientWrapper CreateTopicClient(TopicConfiguration config, IVeStatsDClient statsDClient)
        {
            lock (_lock)
            {
                string topicId = GetHash(config.ConnectionString, config.TopicName);
                if (_topics.ContainsKey(topicId))
                {
                    return _topics[topicId];
                }
                return InstantiateTopic(config, statsDClient, topicId);
            }
        }

        private ITopicClientWrapper InstantiateTopic(TopicConfiguration config, IVeStatsDClient statsDClient, string topicId)
        {
            _creator.SetTopic(config.ConnectionString, config.TopicName, config.Update);
            var messagingFactory = GetMessagingFactory(config.ConnectionString, config.BatchFlushInterval);
            var client = messagingFactory.CreateTopicClient(config.TopicName);
            var topic = new TopicClientWrapper(client, statsDClient);
            _topics.Add(topicId, topic);
            return topic;
        }

        private string GetHash(string connectionString, string topicName)
        {
            return string.Concat(connectionString, topicName);
        }

        private static MessagingFactory GetMessagingFactory(string connectionString, TimeSpan? batchFlushInterval)
        {
            var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);
            var mfs = new MessagingFactorySettings
            {
                TokenProvider = namespaceManager.Settings.TokenProvider
            };
            if (batchFlushInterval.HasValue)
            {
                mfs.NetMessagingTransportSettings.BatchFlushInterval = batchFlushInterval.Value;
            }
            var messagingFactory = MessagingFactory.Create(namespaceManager.Address, mfs);
            return messagingFactory;
        }
    }
}