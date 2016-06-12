using System;
using Ve.Metrics.StatsDClient.Abstract;
using Ve.Messaging.Azure.ServiceBus.Publisher.Wrapper;
using Ve.Messaging.Model;

namespace Ve.Messaging.Azure.ServiceBus.Publisher
{
    public class FailoverPublisherClientResolver : IPublisherClientResolver
    {
        private const string FAILOVER_METRIC = "dependencies.servicebus.failover";
        private readonly IFailoverResolver _failoverResolver;
        private readonly ITopicClientWrapper _topicClient;
        private readonly ITopicClientWrapper _failoverClient;
        private readonly IVeStatsDClient _statsDClient;

        public FailoverPublisherClientResolver(IFailoverResolver failoverResolver,
            ITopicClientWrapper topicClient,
            ITopicClientWrapper failoverClient,
            IVeStatsDClient statsDClient)
        {
            _failoverResolver = failoverResolver;
            _topicClient = topicClient;
            _failoverClient = failoverClient;
            _statsDClient = statsDClient;
        }

        public ITopicClientWrapper GetClient()
        {
            if (_failoverResolver.IsInFailover)
            {
                _statsDClient.LogCount(FAILOVER_METRIC);
                return _failoverClient;
            }
            return _topicClient;
        }

        public void ReportFailure(ITopicClientWrapper wrapper, Message message, Exception ex = null)
        {
            _failoverResolver.ReportException();
        }
    }
}