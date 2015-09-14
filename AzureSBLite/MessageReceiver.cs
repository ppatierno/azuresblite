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
    /// Base abstract class for message receiver
    /// </summary>
    public abstract class MessageReceiver : ClientEntity
    {
        /// <summary>
        /// Entity path
        /// </summary>
        public abstract string Path { get; }

        /// <summary>
        /// Messaging factory
        /// </summary>
        internal MessagingFactory MessagingFactory { get; private set; }

        /// <summary>
        /// ID of the logical partition
        /// </summary>
        internal string PartitionId { get; set; }

        /// <summary>
        /// Offeset from which to start receiving messages
        /// </summary>
        internal string StartOffset { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory</param>
        internal MessageReceiver(MessagingFactory factory)
        {
            this.MessagingFactory = factory;
        }

        /// <summary>
        /// Receive event data from the event hub
        /// </summary>
        /// <returns>EventData instance received</returns>
        internal abstract EventData ReceiveEventData();

        /// <summary>
        /// Receive a brokered message
        /// </summary>
        /// <returns>BrokeredMEssage instance received</returns>
        public abstract BrokeredMessage Receive();

        /// <summary>
        /// Complete the receive operation on a message
        /// </summary>
        /// <param name="lockToken">LockToken assigned to the brokered message</param>
        public abstract void Complete(Guid lockToken);

        /// <summary>
        /// Discards the message and relinquishes the message lock ownership
        /// </summary>
        /// <param name="lockToken">LockToken assigned to the brokered message</param>
        public abstract void Abandon(Guid lockToken);

        /// <summary>
        /// Processes a message in an event-driven message pump
        /// </summary>
        /// <param name="callback">The method to invoke when the operation is complete</param>
        /// <param name="options">Options for the message pump</param>
        public abstract void OnMessage(OnMessageAction callback, OnMessageOptions options);
    }
}
