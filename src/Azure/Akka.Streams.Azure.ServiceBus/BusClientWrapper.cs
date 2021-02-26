using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace Akka.Streams.Azure.ServiceBus
{
    internal interface IBusClient
    {
        Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveBatchAsync(int messageCount, TimeSpan serverWaitTime);

        Task SendBatchAsync(IEnumerable<ServiceBusMessage> messages);
    }
    
    internal sealed class QueueClientWrapper : IBusClient
    {
        private readonly ServiceBusSender _sender;
        private readonly ServiceBusReceiver _receiver;

        public QueueClientWrapper(ServiceBusSender sender, ServiceBusReceiver receiver)
        {
            _sender = sender;
            _receiver = receiver;
        }

        public Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveBatchAsync(int messageCount, TimeSpan serverWaitTime)
            => _receiver.ReceiveMessagesAsync(messageCount, serverWaitTime);

        public Task SendBatchAsync(IEnumerable<ServiceBusMessage> messages) => _sender.SendMessagesAsync(messages);
    }

    internal sealed class SubscriptionClientWrapper : IBusClient
    {
        private readonly ServiceBusReceiver _client;

        public SubscriptionClientWrapper(ServiceBusReceiver client)
        {
            _client = client;
        }

        public Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveBatchAsync(int messageCount, TimeSpan serverWaitTime)
            => _client.ReceiveMessagesAsync(messageCount, serverWaitTime);

        public Task SendBatchAsync(IEnumerable<ServiceBusMessage> messages)
        {
            throw new NotImplementedException();
        }
    }

    internal sealed class TopicClientWrapper : IBusClient
    {
        private readonly ServiceBusSender _client;

        public TopicClientWrapper(ServiceBusSender client)
        {
            _client = client;
        }

        public Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveBatchAsync(int messageCount, TimeSpan serverWaitTime)
        {
            throw new NotImplementedException();
        }

        public Task SendBatchAsync(IEnumerable<ServiceBusMessage> messages) => _client.SendMessagesAsync(messages);
    }
}