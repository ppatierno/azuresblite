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

using ppatierno.AzureSBLite.Messaging.Amqp;
using System;

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Factory for handling connection
    /// </summary>
    public abstract class MessagingFactory : ClientEntity
    {
        // messaging factory settings instance
        private MessagingFactorySettings settings;

        /// <summary>
        /// Base address
        /// </summary>
        public Uri Address { get; internal set; }

        /// <summary>
        /// Create a messaging factory based on a address and related settings
        /// </summary>
        /// <param name="address">Base address</param>
        /// <param name="settings">Messaging factory settings</param>
        /// <returns>Messaging factory</returns>
        public static MessagingFactory Create(Uri address, MessagingFactorySettings settings)
        {
            MessagingFactory factory;

            settings.TransportType = TransportType.Amqp;

            if (settings.AmqpTransportSettings == null)
            {
                settings.AmqpTransportSettings = new AmqpTransportSettings();
                settings.AmqpTransportSettings.Port = AmqpTransportSettings.AMQPS_PORT;
                settings.AmqpTransportSettings.TokenProvider = settings.TokenProvider;
            }

            factory = new AmqpMessagingFactory(address, settings.AmqpTransportSettings);
            factory.settings = settings;

            return factory;
        }

        /// <summary>
        /// Create a messaging factory based on a address and token provider
        /// </summary>
        /// <param name="address">Base address</param>
        /// <param name="tokenProvider">Token provider</param>
        /// <returns>Messaging factory</returns>
        public static MessagingFactory Create(Uri address, TokenProvider tokenProvider)
        {
            MessagingFactorySettings factorySettings = new MessagingFactorySettings();

            factorySettings.TokenProvider = tokenProvider;
            factorySettings.TransportType = TransportType.Amqp;

            factorySettings.AmqpTransportSettings = new AmqpTransportSettings();
            factorySettings.AmqpTransportSettings.Port = AmqpTransportSettings.AMQPS_PORT;
            factorySettings.AmqpTransportSettings.TokenProvider = factorySettings.TokenProvider;

            return Create(address, factorySettings);
        }

        /// <summary>
        /// Create a SAS token provider based on key name and access key
        /// </summary>
        /// <param name="sharedAccessKeyName">Key name</param>
        /// <param name="sharedAccessKey">Access key</param>
        /// <returns>SAS token provider</returns>
        private static TokenProvider CreateTokenProvider(string sharedAccessKeyName, string sharedAccessKey)
        {
            if ((sharedAccessKeyName != null) && (sharedAccessKey != null))
                return TokenProvider.CreateSharedAccessSignatureTokenProvider(sharedAccessKeyName, sharedAccessKey);

            return null;
        }

        /// <summary>
        /// Create a messaging factory from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <returns>Messaging factory</returns>
        public static MessagingFactory CreateFromConnectionString(string connectionString)
        {
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(connectionString);

            Uri endpointAddress = builder.Endpoint;
            string sharedAccessKeyName = builder.SharedAccessKeyName;
            string sharedAccessKey = builder.SharedAccessKey;
            string sharedAccessSignature = builder.SharedAccessSignature;

            TokenProvider tokenProvider = null;
            if ((sharedAccessKeyName != null) && (sharedAccessKeyName != string.Empty))
            {
                tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(sharedAccessKeyName, sharedAccessKey);
            }
            if ((sharedAccessSignature != null) && (sharedAccessSignature != string.Empty))
            {
                tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(sharedAccessSignature);
            }

            return Create(endpointAddress, tokenProvider);
        }

        /// <summary>
        /// Create and event hub client instance
        /// </summary>
        /// <param name="path">Path to event hub entity</param>
        /// <returns>Event hub client instance</returns>
        public abstract EventHubClient CreateEventHubClient(string path);

        /// <summary>
        /// Create a message sender instance
        /// </summary>
        /// <param name="path">Path to the entity</param>
        /// <returns>Message sender instance</returns>
        public abstract MessageSender CreateMessageSender(string path);

        /// <summary>
        /// Create a message receiver instance with peek/lock mode
        /// </summary>
        /// <param name="path">Path to the entity</param>
        /// <returns>Message receiver instance</returns>
        public abstract MessageReceiver CreateMessageReceiver(string path);

        /// <summary>
        /// Create a message receiver instance
        /// </summary>
        /// <param name="path">Path to the entity</param>
        /// <param name="receiveMode">Receive mode</param>
        /// <returns>Message receiver instance</returns>
        public abstract MessageReceiver CreateMessageReceiver(string path, ReceiveMode receiveMode);

        /// <summary>
        /// Creates a new queue client
        /// </summary>
        /// <param name="path">Path to the entity</param>
        /// <returns>Queue client instance</returns>
        public abstract QueueClient CreateQueueClient(string path);

        /// <summary>
        /// Creates a new queue client
        /// </summary>
        /// <param name="path">Path to the entity</param>
        /// <param name="receiveMode">Receive mode</param>
        /// <returns>Queue client instance</returns>
        public abstract QueueClient CreateQueueClient(string path, ReceiveMode receiveMode);

        /// <summary>
        /// Creates a new topic client
        /// </summary>
        /// <param name="path">Path to the entity</param>
        /// <returns>Topic client instance</returns>
        public abstract TopicClient CreateTopicClient(string path);

        /// <summary>
        /// Create a new subscription client
        /// </summary>
        /// <param name="topicPath">Path to the topic related entity</param>
        /// <param name="name">Name of the subscription</param>
        /// <returns>Subscription client instance</returns>
        public abstract SubscriptionClient CreateSubscriptionClient(string topicPath, string name);

        /// <summary>
        /// Create a new subscription client
        /// </summary>
        /// <param name="topicPath">Path to the topic related entity</param>
        /// <param name="name">Name of the subscription</param>
        /// <param name="receiveMode">Receive mode</param>
        /// <returns>Subscription client instance</returns>
        public abstract SubscriptionClient CreateSubscriptionClient(string topicPath, string name, ReceiveMode receiveMode);

        /// <summary>
        /// Create a receiver instance
        /// </summary>
        /// <param name="path">Path to the entity</param>
        /// <param name="consumerGroupName">Consumer group name</param>
        /// <param name="partitionId">ID for a logical partition</param>
        /// <param name="startingOffset">Starting offset at which to start receiving messages</param>
        /// <returns>Message receiver instance</returns>
        public abstract MessageReceiver CreateReceiver(string path, string consumerGroupName, string partitionId, string startingOffset);
        
        /// <summary>
        /// Open connection to the service bus
        /// </summary>
        /// <returns>Connection opened</returns>
        internal abstract bool OpenConnection();
    }
}
