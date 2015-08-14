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
    /// Base abstract class for subscription client
    /// </summary>
    public abstract class SubscriptionClient : ClientEntity
    {
        /// <summary>
        /// Path to the topic related entity
        /// </summary>
        public string TopicPath { get; private set; }

        /// <summary>
        /// Name of the subscription
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Path to the subscription entity
        /// </summary>
        internal string SubscriptionPath { get; private set; }

        /// <summary>
        /// Internal messaging factory
        /// </summary>
        internal MessagingFactory MessagingFactory { get; private set; }

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
        /// <param name="topicPath">Path to the topic related entity</param>
        /// <param name="name">Name of the subscription</param>
        /// <param name="receiveMode">Receive mode</param>
        internal SubscriptionClient(MessagingFactory factory, string topicPath, string name, ReceiveMode receiveMode)
        {
            this.MessagingFactory = factory;
            this.TopicPath = topicPath;
            this.Name = name;
            this.Mode = receiveMode;
            this.SubscriptionPath = topicPath + "/Subscriptions/" + name;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory</param>
        /// <param name="subscriptionPath">Path to the subscription entity</param>
        /// <param name="receiveMode">Receive mode</param>
        internal SubscriptionClient(MessagingFactory factory, string subscriptionPath, ReceiveMode receiveMode)
        {
            this.MessagingFactory = factory;
            this.SubscriptionPath = subscriptionPath;
            this.Mode = receiveMode;
        }

        /// <summary>
        /// Create a subscription client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="topicPath">Path to the topic related entity</param>
        /// <param name="name">Name of the subscription</param>
        /// <returns>Instance of TopicClient class</returns>
        public static SubscriptionClient CreateFromConnectionString(string connectionString, string topicPath, string name)
        {
            return CreateFromConnectionString(connectionString, topicPath, name, ReceiveMode.PeekLock);
        }

        /// <summary>
        /// Create a subscription client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="topicPath">Path to the topic related entity</param>
        /// <param name="name">Name of the subscription</param>
        /// <param name="receiveMode">Receive mode</param>
        /// <returns>Instance of TopicClient class</returns>
        public static SubscriptionClient CreateFromConnectionString(string connectionString, string topicPath, string name, ReceiveMode receiveMode)
        {
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);
            SubscriptionClient client = factory.CreateSubscriptionClient(topicPath, name, receiveMode);
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
    }
}
