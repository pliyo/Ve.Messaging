using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Ve.Metrics.StatsDClient.Abstract;
using Ve.Messaging.Azure.ServiceBus.Core.Wrapper;
using Ve.Messaging.Azure.ServiceBus.Publisher;
using Ve.Messaging.Serializer;
using Ve.Messaging.Publisher;

namespace Ve.Messaging.Azure.ServiceBus.Core
{
    public class PublisherFactory
    {
        private readonly IVeStatsDClient _statsDClient;
        private readonly IFailoverResolver _failoverResolver;
        private readonly ISerializer _serializer;
        private readonly ITopicClientCreator _topicClientCreator;

        public PublisherFactory(IVeStatsDClient statsDClient,
            IFailoverResolver failoverResolver,
            ISerializer serializer,
            ITopicClientCreator topicClientCreator)
        {
            _statsDClient = statsDClient;
            _failoverResolver = failoverResolver;
            _serializer = serializer;
            _topicClientCreator = topicClientCreator;
        }

        public IMessagePublisher CreatePublisher(ServiceBusPublisherConfiguration config)
        {
            IMessagePublisher publisher;
            switch (config.ServiceBusPublisherStrategy)
            {
                case ServiceBusPublisherStrategy.Simple:
                    publisher = GetSimpleMessagePublisher(config);
                    break;

                case ServiceBusPublisherStrategy.FailOverEnable:
                    publisher = GetFailoverMessagePublisher(config);
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
            return publisher;
        }

        private IMessagePublisher GetSimpleMessagePublisher(ServiceBusPublisherConfiguration config)
        {
            var simpleTopicClient = _topicClientCreator.CreateTopicClient(config.PrimaryConfiguration,
                _statsDClient);
            var resolver = new SimplePublisherClientResolver(simpleTopicClient);
            return new MessagePublisher(resolver, _serializer);
        }

        private IMessagePublisher GetFailoverMessagePublisher(ServiceBusPublisherConfiguration config)
        {
            var primaryTopicClient = _topicClientCreator.CreateTopicClient(config.PrimaryConfiguration,
                _statsDClient);

            var failoverTopicClient = _topicClientCreator.CreateTopicClient(config.FailOverConfiguration,
                _statsDClient);

            var failoverPublisher = new FailoverPublisherClientResolver(
                _failoverResolver,
                primaryTopicClient,
                failoverTopicClient,
                _statsDClient);

           return new MessagePublisher(failoverPublisher, _serializer);
        }
    }
}