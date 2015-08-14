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

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Base abstract class for message sender
    /// </summary>
    public abstract class MessageSender : ClientEntity
    {
        /// <summary>
        /// Messaging factory
        /// </summary>
        internal MessagingFactory MessagingFactory { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        internal MessageSender(MessagingFactory factory)
        {
            this.MessagingFactory = factory;
        }

        /// <summary>
        /// Send event data to the event hub
        /// </summary>
        /// <param name="data">EventData instance to send</param>
        internal abstract void SendEventData(EventData data);

        /// <summary>
        /// Send a brokered message
        /// </summary>
        /// <param name="message">Brokered message to send</param>
        public abstract void Send(BrokeredMessage brokeredMessage);
    }
}
