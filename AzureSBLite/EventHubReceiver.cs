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
    /// Event hub receiver
    /// </summary>
    public sealed class EventHubReceiver : ClientEntity
    {
        /// <summary>
        /// Path of the event hub
        /// </summary>
        public string Path { get; private set; }

        /// <summary>
        /// Consumer name
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// ID of the partition
        /// </summary>
        public string PartitionId { get; private set; }

        /// <summary>
        /// Starting offset at which to start receiving messages
        /// </summary>
        public string StartingOffset { get; private set; }

        /// <summary>
        /// Starting date/time offset at which to start receiving messages
        /// </summary>
        public DateTime StartingDateTimeUtc { get; private set; }

        /// <summary>
        /// Internal messaging factory
        /// </summary>
        internal MessagingFactory MessagingFactory { get; private set; }

        /// <summary>
        /// Internal receiver
        /// </summary>
        internal MessageReceiver Receiver { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory</param>
        /// <param name="path">Path to the event hub</param>
        /// <param name="consumerName">Consumer name</param>
        /// <param name="partitionId">ID for a logical partition of an event hub</param>
        /// <param name="startingDateTimeUtc">Starting date/time offset at which to start receiving messages</param>
        internal EventHubReceiver(MessagingFactory factory, string path, string consumerName, string partitionId, DateTime startingDateTimeUtc)
        {
            this.MessagingFactory = factory;
            this.Path = path;
            this.Name = consumerName;
            this.PartitionId = partitionId;
            this.StartingDateTimeUtc = startingDateTimeUtc;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory</param>
        /// <param name="path">Path to the event hub</param>
        /// <param name="consumerName">Consumer name</param>
        /// <param name="partitionId">ID for a logical partition of an event hub</param>
        /// <param name="startingOffset">Starting offset at which to start receiving messages</param>
        internal EventHubReceiver(MessagingFactory factory, string path, string consumerName, string partitionId, string startingOffset)
        {
            this.MessagingFactory = factory;
            this.Path = path;
            this.Name = consumerName;
            this.PartitionId = partitionId;
            this.StartingOffset = startingOffset;
            // note : the MaxValue is the default value and it means NO value (as null)
            //        DateTime? nullable types aren't supported in .Net Micro Framework
            this.StartingDateTimeUtc = DateTime.MaxValue;
        }

        /// <summary>
        /// Receive data from the event hub
        /// </summary>
        /// <returns>EventData instance received</returns>
        public EventData Receive()
        {
            this.CreateReceiver();
            return this.Receiver.ReceiveEventData();
        }

        /// <summary>
        /// Create the internal receiver for event hub
        /// </summary>
        private void CreateReceiver()
        {
            if (this.Receiver == null)
            {
                this.Receiver = this.MessagingFactory.CreateReceiver(this.Path, this.Name, this.PartitionId, this.StartingOffset, this.StartingDateTimeUtc);
            }
        }

        #region ClientEntity ...

        public override void Close()
        {
            if (this.Receiver != null)
            {
                this.Receiver.Close();
                this.isClosed = true;
            }
        }

        #endregion
    }
}
