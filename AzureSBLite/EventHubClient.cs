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

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Base abstract class for event hub client
    /// </summary>
    public abstract class EventHubClient : ClientEntity
    {
        /// <summary>
        /// Path to the event hub entity
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
        internal EventHubClient(MessagingFactory factory, string path)
        {
            this.MessagingFactory = factory;
            this.Path = path;
        }

        /// <summary>
        /// Create an event hub client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <returns>Instace of EventHubClient class</returns>
        public static EventHubClient CreateFromConnectionString(string connectionString)
        {
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(connectionString);
            return CreateFromConnectionString(connectionString, builder.EntityPath);
        }

        /// <summary>
        /// Create an event hub client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <param name="path">Path to the event hub entity</param>
        /// <returns>Instace of EventHubClient class</returns>
        public static EventHubClient CreateFromConnectionString(string connectionString, string path)
        {
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);
            EventHubClient client = factory.CreateEventHubClient(path);
            client.MessagingFactory = factory;

            return client;
        }

        /// <summary>
        /// Creates an event hub sender for the specified event hub partition
        /// </summary>
        /// <param name="partitionId">ID of the partition</param>
        /// <returns>Instance of EventHubSender class</returns>
        public EventHubSender CreatePartitionedSender(string partitionId)
        {
            return new EventHubSender(this.MessagingFactory, this.Path, partitionId);
        }

        /// <summary>
        /// Creates an event hub sender for the specified event hub publisher
        /// </summary>
        /// <param name="publisher">Publisher identifier</param>
        /// <returns>Instance of EventHubSender class</returns>
        public EventHubSender CreateSender(string publisher)
        {
            return new EventHubSender(this.MessagingFactory, Fx.Format("{0}/Publishers/{1}", this.Path, publisher), null);
        }

        /// <summary>
        /// Returns the consumer group with the specified name
        /// </summary>
        /// <param name="groupName">Name of the group</param>
        /// <returns>Instance of the consumer group</returns>
        public EventHubConsumerGroup GetConsumerGroup(string groupName)
        {
            return new EventHubConsumerGroup(this.MessagingFactory, this.Path, groupName);
        }

        /// <summary>
        /// Returns the default consumer group
        /// </summary>
        /// <returns>Instance of the default consumer group</returns>
        public EventHubConsumerGroup GetDefaultConsumerGroup()
        {
            return this.GetConsumerGroup(EventHubConsumerGroup.DEFAULT_GROUP_NAME);
        }

        /// <summary>
        /// Send data to the event hub
        /// </summary>
        /// <param name="data">EventData instance to send</param>
        public void Send(EventData data)
        {
            this.CreateSender();
            this.Sender.SendEventData(data);
        }

        /// <summary>
        /// Create the internal sender for event hub
        /// </summary>
        /// <returns>Instance of internal sender</returns>
        protected abstract MessageSender CreateSender();
    }
}
