using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Ve.Metrics.StatsDClient.Abstract;

namespace Ve.Messaging.Azure.ServiceBus.Publisher.Wrapper
{
    public class TopicClientWrapper : ITopicClientWrapper
    {
        private readonly TopicClient _topicClient;
        private readonly IVeStatsDClient _statsd;

        public TopicClientWrapper(TopicClient topicClient, IVeStatsDClient statsd)
        {
            _topicClient = topicClient;
            _statsd = statsd;
        }

        public virtual async Task SendAsync(BrokeredMessage message)
        {
            var stopwatch = Stopwatch.StartNew();
            _statsd.LogCount("dependencies.servicebus.send");
            _statsd.LogGauge("dependencies.servicebus.messagesize", (int)message.Size);

            await _topicClient.SendAsync(message).ConfigureAwait(false);

            stopwatch.Stop();
            _statsd.LogTiming("dependencies.servicebus.send", stopwatch.ElapsedMilliseconds);
        }
    }
}
