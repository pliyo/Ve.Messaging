using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using Ve.Messaging.Publisher;
using Ve.Messaging.Serializer;
using Ve.Messaging.Model;

namespace Ve.Messaging.Azure.ServiceBus.Publisher
{
    /// <summary>
    /// TODO: Add to a buffer
    /// </summary>
    public class MessagePublisher : IMessagePublisher
    {
        private readonly IPublisherClientResolver _publisherClientResolver;
        private readonly ISerializer _serializer;

        public MessagePublisher(IPublisherClientResolver publisherClientResolver, ISerializer serializer)
        {
            _publisherClientResolver = publisherClientResolver;
            _serializer = serializer;
        }

        public async Task SendAsync(Message message)
        {
            var topicClient = _publisherClientResolver.GetClient();
            try
            {
                await topicClient.SendAsync(SerializeToBrokeredMessage(message)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _publisherClientResolver.ReportFailure(topicClient, message, ex);
            }
        }

        public async Task SendBatchAsync(IEnumerable<Message> messages)
        {
            foreach (var message in messages)
            {
                await SendAsync(message).ConfigureAwait(false);
            }
        }

        private BrokeredMessage SerializeToBrokeredMessage(Message message)
        {
            var body = _serializer.Serialize(message.Content);
            var brokeredMessage = new BrokeredMessage(body);
            brokeredMessage.SessionId = message.SessionId;
            brokeredMessage.Label = message.Label;

            if (message.Properties != null && message.Properties.Count > 0)
            {
                foreach (var item in message.Properties)
                {
                    brokeredMessage.Properties.Add(item.Key, item.Value);
                }
            }
            return brokeredMessage;
        }
    }
}