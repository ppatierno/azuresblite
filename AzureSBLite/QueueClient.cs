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

using ppatierno.AzureSBLite.Utility;
using System;

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Base abstract class for queue client
    /// </summary>
    public abstract class QueueClient : ClientEntity
    {
        /// <summary>
        /// Path to the queue entity
        /// </summary>
        public string Path { get; private set; }

        /// <summary>
        /// Internal messaging factory
        /// </summary>
        internal MessagingFactory MessagingFactory { get; private set; }

        /// <summary>
        /// Internal sender
        /// </summary>
        internal MessageSender Sender { get; set; }

        /// <summary>
        /// Internal receiver
        /// </summary>
        internal MessageReceiver Receiver { get; set; }

        /// <summary>
        /// Receive mode
        /// </summary>
        public ReceiveMode Mode { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory</param>
        /// <param name="path">Path to the event hub entity</param>
        /// <param name="receiveMode">Receive mode</param>
        internal QueueClient(MessagingFactory factory, string path, ReceiveMode receiveMode)
        {
            this.MessagingFactory = factory;
            this.Path = path;
            this.Mode = receiveMode;
        }

        /// <summary>
        /// Create a queue client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <returns>Instace of QueueClient class</returns>
        public static QueueClient CreateFromConnectionString(string connectionString)
        {
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(connectionString);
            return CreateFromConnectionString(connectionString, builder.EntityPath);
        }

        /// <summary>
        /// Create a queue client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="path">Path to the queue entity</param>
        /// <returns>Instace of QueueClient class</returns>
        public static QueueClient CreateFromConnectionString(string connectionString, string path)
        {
            return CreateFromConnectionString(connectionString, path, ReceiveMode.PeekLock);
        }

        /// <summary>
        /// Create a queue client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="path">Path to the queue entity</param>
        /// <param name="receiveMode">Receive mode</param>
        /// <returns>Instace of QueueClient class</returns>
        public static QueueClient CreateFromConnectionString(string connectionString, string path, ReceiveMode receiveMode)
        {
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);
            QueueClient client = factory.CreateQueueClient(path, receiveMode);
            client.MessagingFactory = factory;

            return client;
        }

        /// <summary>
        /// Receive message from the queue
        /// </summary>
        /// <returns>BrokeredMessage instance received</returns>
        public BrokeredMessage Receive()
        {
            this.CreateReceiver();
            return this.Receiver.Receive();
        }

        /// <summary>
        /// Send message to the queue
        /// </summary>
        /// <param name="message">BrokeredMessage instance to send</param>
        public void Send(BrokeredMessage message)
        {
            this.CreateSender();
            this.Sender.Send(message);
        }

        /// <summary>
        /// Discards the message and relinquishes the message lock ownership
        /// </summary>
        /// <param name="lockToken">LockToken assigned to the brokered message</param>
        public void Abandon(Guid lockToken)
        {
            if (this.Receiver != null)
            {
                this.Receiver.Abandon(lockToken);
            }
        }

        /// <summary>
        /// Complete the receive operation on a message
        /// </summary>
        /// <param name="lockToken">LockToken assigned to the brokered message</param>
        public void Complete(Guid lockToken)
        {
            if (this.Receiver != null)
            {
                this.Receiver.Complete(lockToken);
            }
        }

        /// <summary>
        /// Processes a message in an event-driven message pump
        /// </summary>
        /// <param name="callback">The method to invoke when the operation is complete</param>
        public void OnMessage(OnMessageAction callback)
        {
            OnMessageOptions opions = new OnMessageOptions();
            this.OnMessage(callback, opions);
        }

        /// <summary>
        /// Processes a message in an event-driven message pump
        /// </summary>
        /// <param name="callback">The method to invoke when the operation is complete</param>
        /// <param name="options">Options for the message pump</param>
        public void OnMessage(OnMessageAction callback, OnMessageOptions options)
        {
            this.CreateReceiver();
            this.Receiver.OnMessage(callback, options);
        }

        /// <summary>
        /// Create the internal receiver for queue
        /// </summary>
        /// <returns>Instance of internal receiver</returns>
        protected abstract MessageReceiver CreateReceiver();

        /// <summary>
        /// Create the internal sender for queue
        /// </summary>
        /// <returns>Instance of internal sender</returns>
        protected abstract MessageSender CreateSender();
    }
}
