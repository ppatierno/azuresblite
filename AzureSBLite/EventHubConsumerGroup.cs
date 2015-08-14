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
    /// Consumer group within an event hub
    /// </summary>
    public sealed class EventHubConsumerGroup : ClientEntity
    {
        #region Constants ...

        // name for the default consumer group
        public const string DEFAULT_GROUP_NAME = "$Default";

        #endregion

        /// <summary>
        /// Gets the event hub path
        /// </summary>
        public string EventHubPath { get; private set; }

        /// <summary>
        /// Gets the name of the consumer group
        /// </summary>
        public string GroupName { get; private set; }

        /// <summary>
        /// Internal messaging factory
        /// </summary>
        internal MessagingFactory MessagingFactory { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory</param>
        /// <param name="eventHubPath">Path of the event hub</param>
        /// <param name="groupName">Name of the consumer group</param>
        internal EventHubConsumerGroup(MessagingFactory factory, string eventHubPath, string groupName)
        {
            this.MessagingFactory = factory;
            this.EventHubPath = eventHubPath;
            this.GroupName = groupName;
        }

        /// <summary>
        /// Create an event hub receiver for a specified partition
        /// </summary>
        /// <param name="partitionId">ID of the partition</param>
        /// <returns>Instance of EventHubReceiver class</returns>
        public EventHubReceiver CreateReceiver(string partitionId)
        {
            return new EventHubReceiver(this.MessagingFactory, Fx.Format("{0}/ConsumerGroups/{1}/Partitions/{2}", this.EventHubPath, this.GroupName, partitionId), this.GroupName, partitionId, null);
        }

        /// <summary>
        /// Create an event hub receiver for a specified partition from a starting offset
        /// </summary>
        /// <param name="partitionId">ID of the partition</param>
        /// <param name="startingOffset">Starting offset at which to start receiving messages</param>
        /// <returns>Instance of EventHubReceiver class</returns>
        public EventHubReceiver CreateReceiver(string partitionId, string startingOffset)
        {
            return new EventHubReceiver(this.MessagingFactory, Fx.Format("{0}/ConsumerGroups/{1}/Partitions/{2}", this.EventHubPath, this.GroupName, partitionId), this.GroupName, partitionId, startingOffset);
        }
    }
}
