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

using System;

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Base abstract class for topic client
    /// </summary>
    public abstract class TopicClient : ClientEntity
    {
        /// <summary>
        /// Path to the topic entity
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
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory</param>
        /// <param name="path">Path to the event hub entity</param>
        internal TopicClient(MessagingFactory factory, string path)
        {
            this.MessagingFactory = factory;
            this.Path = path;
        }

        /// <summary>
        /// Create a topic client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <returns>Instace of TopicClient class</returns>
        public static TopicClient CreateFromConnectionString(string connectionString)
        {
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(connectionString);
            return CreateFromConnectionString(connectionString, builder.EntityPath);
        }

        /// <summary>
        /// Create a topic client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="path">Path to the queue entity</param>
        /// <returns>Instace of TopicClient class</returns>
        public static TopicClient CreateFromConnectionString(string connectionString, string path)
        {
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);
            TopicClient client = factory.CreateTopicClient(path);
            client.MessagingFactory = factory;

            return client;
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
        /// Create the internal sender for queue
        /// </summary>
        /// <returns>Instance of internal sender</returns>
        protected abstract MessageSender CreateSender();
    }
}
