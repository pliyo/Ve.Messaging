//using NSubstitute;
//using Should;
//using Ve.Metrics.StatsDClient.Abstract;
//using Ve.Platypus.AzureServiceBus.Abstract.Subscription;
//using Ve.Platypus.AzureServiceBus.Abstract.Topic;
//using Ve.Platypus.AzureServiceBus.Core;
//using Ve.Platypus.AzureServiceBus.Core.Configuration;
//using Ve.Platypus.Serialisers.Abstract;
//using Ve.Platypus.Serialisers.PlainText;
//using Xunit;

//namespace Ve.Platypus.AzureServiceBus.Tests.Integration
//{
//    public class TopicClientShould
//    {
//        private readonly INamespaceCreator _namespaceCreator;
//        private readonly ISerializer _serializer;
//        private readonly IVeStatsDClient _statsd;
//        private readonly string _connectionString;
//        private readonly string _topicName;
//        private readonly string _subscriptionName;

//        public TopicClientShould()
//        {
//            _namespaceCreator = new NamespaceCreator();
//            _serializer = new PlainTextSerialiser();
//            _statsd = Substitute.For<IVeStatsDClient>();

//            _connectionString =
//                "Endpoint=sb://v-dv-dtrc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DVtCFHs+H8yu5npAmNShvoMZRowGBLM+6AFUROn6Lsg=";
//            _topicName = "vecapture";
//            _subscriptionName = "validated-received";
//        }

//        private ISubscriptionClientWrapper GetSubscriptionClient()
//        {
//            var configuration = new Configuration(_connectionString, _topicName, _subscriptionName);

//            var topic = new TopicManager(_namespaceCreator, _statsd);
//            ISubscriptionClientCreate subs = new SubscriptionManager(_namespaceCreator, _statsd);
//            var client = subs.GetClient(configuration);

//            return client;
//        }

//        private ITopicClientWrapper GetTopicClient()
//        {
//            var configuration = new Configuration(_connectionString, _topicName);

//            var topic = new TopicManager(_namespaceCreator, _statsd);
//            var client = topic.GetClient(configuration, false);

//            return client;
//        }

//        [Fact]
//        public void Send_A_Message()
//        {
//            var client = GetTopicClient();

//            var message = "text";
//            var obj = _serializer.Serialize(new { text = message });

//            client.SendAsync(obj).Wait();
//        }

//        [Fact]
//        public void Send_And_Receive_Message()
//        {
//            var topicClient = GetTopicClient();
//            var subscriptionClient = GetSubscriptionClient();
//            var obj = _serializer.Serialize("test");
//            topicClient.SendAsync(obj).Wait();

//            var result = subscriptionClient.RetrieveMessages(100, 10);

//            result.Count.ShouldBeGreaterThan(0);
//        }
//    }
//}