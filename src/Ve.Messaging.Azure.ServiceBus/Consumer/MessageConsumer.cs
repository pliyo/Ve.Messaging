using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.IO;
using Ve.Messaging.Consumer;
using Ve.Messaging.Model;
using Ve.Messaging.Serializer;

namespace Ve.Messaging.Azure.ServiceBus.Consumer
{
    public class MessageConsumer : IMessageConsumer
    {
        private readonly SubscriptionClient _client;
        private readonly ISerializer _serializer;

        public MessageConsumer(SubscriptionClient client, ISerializer serializer)
        {
            _client = client;
            _serializer = serializer;
        }

        public List<Message> RetrieveMessages(int messageAmount, int timeout)
        {
            var brokeredMessages = _client.ReceiveBatch(messageAmount, TimeSpan.FromSeconds(timeout));
            var messages = new List<Message>();

            foreach (var brokeredMessage in brokeredMessages)
            {
                Message message = new Message();

                var stream = brokeredMessage.GetBody<Stream>();
                message.Content = _serializer.ParseMessage<object>(stream);
                message.Label = brokeredMessage.Label;
                message.SessionId = brokeredMessage.SessionId;
                message.Properties = new Dictionary<string, object>(brokeredMessage.Properties);
                messages.Add(message);
            }

            return messages;
        }
    }
}