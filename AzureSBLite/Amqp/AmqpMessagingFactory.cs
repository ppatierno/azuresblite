/*
Copyright (c) 2015 Paolo Patierno

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using Amqp;
using System;

namespace ppatierno.AzureSBLite.Messaging.Amqp
{
    /// <summary>
    /// Factory for handling AMQP connection
    /// </summary>
    internal sealed class AmqpMessagingFactory : MessagingFactory
    {
        /// <summary>
        /// AMQP connection to the service bus
        /// </summary>
        internal Connection Connection { get; private set; }

        // AMQP settings
        internal AmqpTransportSettings TransportSettings { get; private set; }

        // base address
        private Address amqpAddress;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="address">Base address to service bus</param>
        public AmqpMessagingFactory(string address)
        {
            this.amqpAddress = new Address(address);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="baseAddress">Base address to service bus</param>
        /// <param name="settings">AMQP transport settings</param>
        public AmqpMessagingFactory(Uri baseAddress, AmqpTransportSettings settings)
        {
            this.Address = baseAddress;
            this.TransportSettings = settings;

            SharedAccessSignatureTokenProvider sasTokenProvider = (SharedAccessSignatureTokenProvider)this.TransportSettings.TokenProvider;
            this.amqpAddress = new Address(this.Address.Host, this.TransportSettings.Port, sasTokenProvider.KeyName, sasTokenProvider.SharedAccessKey);
        }

        #region MessagingFactory ...

        public override EventHubClient CreateEventHubClient(string path)
        {
            return new AmqpEventHubClient(this, path);
        }

        public override MessageSender CreateMessageSender(string path)
        {
            return new AmqpMessageSender(this, path);
        }

        public override MessageReceiver CreateMessageReceiver(string path)
        {
            return this.CreateMessageReceiver(path, ReceiveMode.PeekLock);
        }

        public override MessageReceiver CreateMessageReceiver(string path, ReceiveMode receiveMode)
        {
            return new AmqpMessageReceiver(this, path, receiveMode);
        }

        public override QueueClient CreateQueueClient(string path)
        {
            return this.CreateQueueClient(path, ReceiveMode.PeekLock);
        }

        public override QueueClient CreateQueueClient(string path, ReceiveMode receiveMode)
        {
            return new AmqpQueueClient(this, path, receiveMode);
        }

        public override TopicClient CreateTopicClient(string path)
        {
            return new AmqpTopicClient(this, path);
        }

        public override SubscriptionClient CreateSubscriptionClient(string topicPath, string name)
        {
            return this.CreateSubscriptionClient(topicPath, name, ReceiveMode.PeekLock);
        }

        public override SubscriptionClient CreateSubscriptionClient(string topicPath, string name, ReceiveMode receiveMode)
        {
            return new AmqpSubscriptionClient(this, topicPath, name, receiveMode);
        }

        public override MessageReceiver CreateReceiver(string path, string consumerGroupName, string partitionId, string startingOffset)
        {
            AmqpMessageReceiver receiver = new AmqpMessageReceiver(this, path);
            receiver.PartitionId = partitionId;
            receiver.StartOffset = startingOffset;
            return receiver;
        }

        internal override bool OpenConnection()
        {
            if (this.Connection == null)
            {
                if (this.TransportSettings.TokenProvider.GetType() == typeof(SharedAccessSignatureTokenProvider))
                {
                    SharedAccessSignatureTokenProvider tokenProvider = (SharedAccessSignatureTokenProvider)this.TransportSettings.TokenProvider;
                    if ((tokenProvider.ShareAccessSignature != null) && (tokenProvider.ShareAccessSignature != string.Empty))
                    {
                        this.Connection = new Connection(this.amqpAddress, global::Amqp.Sasl.SaslProfile.External, null, null);
                    }
                    else
                    {
                        this.Connection = new Connection(this.amqpAddress);
                    }
                }
            }
            return true;
        }

        #endregion

        #region ClientEntity ...

        public override void Close()
        {
            if (this.Connection != null)
                this.Connection.Close();
            this.isClosed = true;
        }

        #endregion
    }
}
