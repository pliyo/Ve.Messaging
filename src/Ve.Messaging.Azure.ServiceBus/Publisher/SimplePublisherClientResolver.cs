using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ve.Messaging.Azure.ServiceBus.Publisher;
using Ve.Messaging.Azure.ServiceBus.Publisher.Wrapper;
using Ve.Messaging.Model;

namespace Ve.Messaging.Azure.ServiceBus.Core.Wrapper
{
    public class SimplePublisherClientResolver : IPublisherClientResolver
    {
        private readonly ITopicClientWrapper _topicClient;

        public SimplePublisherClientResolver(ITopicClientWrapper topicClient)
        {
            _topicClient = topicClient;
        }
        public ITopicClientWrapper GetClient()
        {
            return _topicClient;
        }

        public void ReportFailure(ITopicClientWrapper wrapper, Message message, Exception ex = null)
        {
        }
    }
}
