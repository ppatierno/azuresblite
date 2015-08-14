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
    /// Subscription client for AMQP protocol
    /// </summary>
    internal sealed class AmqpSubscriptionClient : SubscriptionClient
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        /// <param name="subscriptionPath">Path to subscription entity</param>
        /// <param name="receiveMode">Receive mode</param>
        internal AmqpSubscriptionClient(AmqpMessagingFactory factory, string subscriptionPath, ReceiveMode receiveMode)
            : base(factory, subscriptionPath, receiveMode)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        /// <param name="topicPath">Path to the topic related entity</param>
        /// <param name="name">Name of the subscription</param>
        /// <param name="receiveMode">Receive mode</param>
        internal AmqpSubscriptionClient(AmqpMessagingFactory factory, string topicPath, string name, ReceiveMode receiveMode)
            : base(factory, topicPath, name, receiveMode)
        {
        }

        #region SubscriptionClient ...

        protected override MessageReceiver CreateReceiver()
        {
            if (this.Receiver == null)
            {
                this.Receiver = this.MessagingFactory.CreateMessageReceiver(this.SubscriptionPath, this.Mode);
            }
            return this.Receiver;
        }

        #endregion

        #region ClientEntity ...

        public override void Close()
        {
            if (this.Receiver != null)
            {
                this.Receiver.Close();
            }
            this.isClosed = true;
        }

        #endregion
    }
}
