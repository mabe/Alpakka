﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Azure.Utils;
using Akka.Streams.Dsl;
using Akka.Streams.Stage;
using Akka.Streams.Supervision;
using Azure.Messaging.ServiceBus;

namespace Akka.Streams.Azure.ServiceBus
{
    /// <summary>
    /// A <see cref="Sink{TIn,TMat}"/> for the Azure ServiceBus
    /// </summary>
    public class ServiceBusSink : GraphStageWithMaterializedValue<SinkShape<IEnumerable<ServiceBusMessage>>, Task>
    {
        #region Logic

        private sealed class Logic : GraphStageLogic
        {
            private readonly ServiceBusSink _sink;
            private readonly Decider _decider;
            private readonly TaskCompletionSource<NotUsed> _completion;
            private Action<Tuple<Task, List<ServiceBusMessage>>> _batchSendCallback;
            private bool _isSendInProgress;

            public Logic(ServiceBusSink sink, Attributes inheritedAttributes, TaskCompletionSource<NotUsed> completion) : base(sink.Shape)
            {
                _sink = sink;
                _completion = completion;
                _decider = inheritedAttributes.GetDeciderOrDefault();
                
                SetHandler(sink.In,
                    onPush: () => TrySend(Grab(_sink.In).ToList()),
                    onUpstreamFinish: () =>
                    {
                        // It is most likely that we receive the finish event before the task from the last batch has finished
                        // so if the task is still running we need to complete the stage later
                        if (!_isSendInProgress)
                            Finish();
                    },
                    onUpstreamFailure: ex =>
                    {
                        _completion.TrySetException(ex);
                        // We have set KeepGoing to true so we need to fail the stage manually
                        FailStage(ex);
                    });
            }

            public override void PreStart()
            {
                // Keep going even if the upstream has finished so that we can process the task from the last batch
                SetKeepGoing(true);
                _batchSendCallback = GetAsyncCallback<Tuple<Task, List<ServiceBusMessage>>>(OnBatchSend);
                // Request the first batch
                Pull(_sink.In);
            }

            private void TrySend(List<ServiceBusMessage> messages)
            {
                _isSendInProgress = true;
                _sink._client.SendBatchAsync(messages).ContinueWith(t => _batchSendCallback(Tuple.Create(t, messages)));
            }

            private void OnBatchSend(Tuple<Task, List<ServiceBusMessage>> t)
            {
                _isSendInProgress = false;
                var task = t.Item1;
                var messages = t.Item2;

                if (task.IsFaulted || task.IsCanceled)
                {
                    switch (_decider(task.Exception))
                    {
                        case Directive.Stop:
                            // Throw
                            _completion.TrySetException(task.Exception);
                            FailStage(task.Exception);
                            break;
                        case Directive.Resume:
                            // Try again
                            TrySend(messages);
                            break;
                        case Directive.Restart:
                            // Take the next batch or complete
                            PullOrComplete();
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                else
                    PullOrComplete();
            }
            
            private void PullOrComplete()
            {
                if (IsClosed(_sink.In))
                    Finish();
                else
                    Pull(_sink.In);
            }
            
            private void Finish()
            {
                _completion.TrySetResult(NotUsed.Instance);
                CompleteStage();
            }
        }

        #endregion

        /// <summary>
        /// Creates a <see cref="Sink{TIn,TMat}"/> for the Azure ServiceBus
        /// </summary>
        /// <param name="client">The client</param>
        /// <returns>The <see cref="Sink{TIn,TMat}"/> for the Azure ServiceBus</returns>
        public static Sink<IEnumerable<ServiceBusMessage>, Task> Create(ServiceBusSender sender, ServiceBusReceiver receiver)
        {
            return Sink.FromGraph(new ServiceBusSink(sender, receiver));
        }

        /// <summary>
        /// Creates a <see cref="Sink{TIn,TMat}"/> for the Azure ServiceBus
        /// </summary>
        /// <param name="client">The client</param>
        /// <returns>The <see cref="Sink{TIn,TMat}"/> for the Azure ServiceBus</returns>
        public static Sink<IEnumerable<ServiceBusMessage>, Task> Create(ServiceBusSender client)
        {
            return Sink.FromGraph(new ServiceBusSink(client));
        }

        private readonly IBusClient _client;

        private ServiceBusSink(IBusClient client)
        {
            _client = client;

            Shape = new SinkShape<IEnumerable<ServiceBusMessage>>(In);
        }

        /// <summary>
        /// Create a new instance of the <see cref="ServiceBusSink"/> 
        /// </summary>
        /// <param name="client">The client</param>
        public ServiceBusSink(ServiceBusSender sender, ServiceBusReceiver receiver) : this(new QueueClientWrapper(sender, receiver))
        {

        }

        /// <summary>
        /// Create a new instance of the <see cref="ServiceBusSink"/> 
        /// </summary>
        /// <param name="client">The client</param>
        public ServiceBusSink(ServiceBusSender client) : this(new TopicClientWrapper(client))
        {

        }

        public Inlet<IEnumerable<ServiceBusMessage>> In { get; } = new Inlet<IEnumerable<ServiceBusMessage>>("EventHubSink.In");

        public override SinkShape<IEnumerable<ServiceBusMessage>> Shape { get; }

        protected override Attributes InitialAttributes { get; } = Attributes.CreateName("EventHubSink");

        public override ILogicAndMaterializedValue<Task> CreateLogicAndMaterializedValue(Attributes inheritedAttributes)
        {
            var completion = new TaskCompletionSource<NotUsed>();
            var logic = new Logic(this, inheritedAttributes, completion);
            return new LogicAndMaterializedValue<Task>(logic, completion.Task); 
        }
    }
}