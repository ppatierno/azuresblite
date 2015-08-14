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

namespace ppatierno.AzureSBLite.Messaging.Amqp
{
    /// <summary>
    /// Queue client for AMQP protocol
    /// </summary>
    internal sealed class AmqpQueueClient : QueueClient
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        /// <param name="path">Path to event hub entity</param>
        /// <param name="receiveMode">Receive mode</param>
        internal AmqpQueueClient(AmqpMessagingFactory factory, string path, ReceiveMode receiveMode)
            : base(factory, path, receiveMode)
        {
        }

        #region QueueClient ...

        protected override MessageReceiver CreateReceiver()
        {
            if (this.Receiver == null)
            {
                this.Receiver = this.MessagingFactory.CreateMessageReceiver(this.Path, this.Mode);
            }
            return this.Receiver;
        }

        protected override MessageSender CreateSender()
        {
            if (this.Sender == null)
            {
                this.Sender = this.MessagingFactory.CreateMessageSender(this.Path);
            }
            return this.Sender;
        }

        #endregion

        #region ClientEntity ...

        public override void Close()
        {
            if (this.Sender != null)
            {
                this.Sender.Close();
            }

            if (this.Receiver != null)
            {
                this.Receiver.Close();
            }
            this.isClosed = true;
        }

        #endregion
    }
}
