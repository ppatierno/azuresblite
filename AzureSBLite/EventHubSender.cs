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
    /// Event hub sender
    /// </summary>
    public sealed class EventHubSender : ClientEntity
    {
        /// <summary>
        /// Path of the event hub
        /// </summary>
        public string Path { get; private set; }

        /// <summary>
        /// ID for a logical partition of an event hub
        /// </summary>
        public string PartitionId { get; private set; }

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
        /// <param name="path">Path of the event hub</param>
        /// <param name="partitionId">ID for a logical partition of an event hub</param>
        internal EventHubSender(MessagingFactory factory, string path, string partitionId)
        {
            this.MessagingFactory = factory;
            this.Path = path;
            this.PartitionId = partitionId;
        }

        /// <summary>
        /// Create an event hub client from a connection string
        /// </summary>
        /// <param name="connectionString">Connection string</param>
        /// <returns>Instace of EventHubSender class</returns>
        public static EventHubSender CreateFromConnectionString(string connectionString)
        {
            MessagingFactory factory = MessagingFactory.CreateFromConnectionString(connectionString);
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder(connectionString);
            return new EventHubSender(factory, Fx.Format("{0}/Publishers/{1}", builder.EntityPath, builder.Publisher), null);
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
        private void CreateSender()
        {
            if (this.Sender == null)
            {
                string path = (this.PartitionId == null) ? this.Path : this.Path + "/Partitions/" + this.PartitionId;
                this.Sender = this.MessagingFactory.CreateMessageSender(path);
            }
        }

        #region ClientEntity ...

        public override void Close()
        {
            if (this.Sender != null)
            {
                this.Sender.Close();
                this.isClosed = true;
            }
        }

        #endregion
    }
}
