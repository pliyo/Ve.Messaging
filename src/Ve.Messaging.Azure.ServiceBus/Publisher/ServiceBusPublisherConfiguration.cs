using System;

namespace Ve.Messaging.Azure.ServiceBus.Publisher
{
    public class ServiceBusPublisherConfiguration
    {
        public ServiceBusPublisherStrategy ServiceBusPublisherStrategy { get; set; }
        public TopicConfiguration PrimaryConfiguration { get; set; }
        public TopicConfiguration FailOverConfiguration { get; set; }
    }

    public class TopicConfiguration
    {
        public TimeSpan? BatchFlushInterval { get; set; }
        public string ConnectionString { get; set; }
        public bool Update { get; set; }
        public string TopicName { get; set; }
    }
}