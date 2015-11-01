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
using Amqp.Framing;
using ppatierno.AzureSBLite.Channel.Security;
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

        internal AmqpTransportSettings TransportSettings
        {
            get { return this.settings; }
        }

        internal IServiceBusSecuritySettings ServiceBusSecuritySettings
        {
            get { return this.settings; }
        }

        // base address
        private Address amqpAddress;

        // AMQP settings
        private readonly AmqpTransportSettings settings;

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
            this.settings = settings;

            SharedAccessSignatureTokenProvider sasTokenProvider = (SharedAccessSignatureTokenProvider)this.ServiceBusSecuritySettings.TokenProvider;
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

        internal override MessageReceiver CreateReceiver(string path, string consumerGroupName, string partitionId, string startingOffset, DateTime startingDateTimeUtc)
        {
            AmqpMessageReceiver receiver = new AmqpMessageReceiver(this, path);
            receiver.PartitionId = partitionId;
            receiver.StartOffset = startingOffset;
            receiver.ReceiverStartTime = startingDateTimeUtc;
            return receiver;
        }

        internal override bool Open(string entity)
        {
            if (this.Connection == null)
            {
                if (this.ServiceBusSecuritySettings.TokenProvider.GetType() == typeof(SharedAccessSignatureTokenProvider))
                {
                    SharedAccessSignatureTokenProvider tokenProvider = (SharedAccessSignatureTokenProvider)this.ServiceBusSecuritySettings.TokenProvider;
                    if ((tokenProvider.ShareAccessSignature != null) && (tokenProvider.ShareAccessSignature != string.Empty))
                    {
                        this.Connection = new Connection(this.amqpAddress, global::Amqp.Sasl.SaslProfile.External, null, null);

                        // send CBS token for the entity
                        return this.PutCbsToken(tokenProvider.ShareAccessSignature, entity);
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

        /// <summary>
        /// Send Claim Based Security (CBS) token
        /// </summary>
        /// <param name="shareAccessSignature">Shared access signature (token) to send</param>
        /// <param name="entity">Entity name</param>
        private bool PutCbsToken(string shareAccessSignature, string entity)
        {
            bool result = true;
            Session session = new Session(this.Connection);

            string cbsReplyToAddress = "cbs-reply-to";
            var cbsSender = new SenderLink(session, "cbs-sender", "$cbs");
            var cbsReceiver = new ReceiverLink(session, cbsReplyToAddress, "$cbs");

            // construct the put-token message
            var request = new Message(shareAccessSignature);
            request.Properties = new Properties();
            request.Properties.MessageId = Guid.NewGuid().ToString();
            request.Properties.ReplyTo = cbsReplyToAddress;
            request.ApplicationProperties = new ApplicationProperties();
            request.ApplicationProperties["operation"] = "put-token";
            request.ApplicationProperties["type"] = "servicebus.windows.net:sastoken";
            request.ApplicationProperties["name"] = Fx.Format("amqp://{0}/{1}", this.Address.Host, entity);
            cbsSender.Send(request);

            // receive the response
            var response = cbsReceiver.Receive();
            if (response == null || response.Properties == null || response.ApplicationProperties == null)
            {
                result = false;
            }
            else
            {
                int statusCode = (int)response.ApplicationProperties["status-code"];
                if (statusCode != (int)202 && statusCode != (int)200) // !Accepted && !OK
                {
                    result = false;
                }
            }

            // the sender/receiver may be kept open for refreshing tokens
            cbsSender.Close();
            cbsReceiver.Close();
            session.Close();

            return result;
        }
    }
}
