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
using Amqp;
using Amqp.Framing;

namespace ppatierno.AzureSBLite.Messaging.Amqp
{
    /// <summary>
    /// Message sender for AMQP protocol
    /// </summary>
    internal sealed class AmqpMessageSender : MessageSender
    {
        // AMQP messaging factory
        private AmqpMessagingFactory factory;

        // session and link of AMQP protocol
        private Session session;
        private SenderLink link;

        // entity to connect (AMQP node)
        private string entity;

        /// <summary>
        /// Construcotr
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        /// <param name="entity">Entity path</param>
        internal AmqpMessageSender(AmqpMessagingFactory factory, string entity) 
            : base(factory)
        {
            this.factory = factory;
            this.entity = entity;
        }

        #region MessageSender ...

        public override string Path
        {
            get
            {
                return this.entity;
            }
        }

        internal override void SendEventData(EventData data)
        {
            if (this.factory.Open(this.entity))
            {
                if (this.session == null)
                {
                    this.session = new Session(this.factory.Connection);
                    this.link = new SenderLink(this.session, "amqp-send-link " + this.entity, this.entity);   
                }

                Message message = data.ToAmqpMessage();
                this.link.Send(message);
            }
        }

        public override void Send(BrokeredMessage brokeredMessage)
        {
            if (this.factory.Open(this.entity))
            {
                if (this.session == null)
                {
                    this.session = new Session(this.factory.Connection);
                    this.link = new SenderLink(this.session, "amqp-send-link " + this.entity, this.entity);
                }

                Message message = brokeredMessage.ToAmqpMessage();
                this.link.Send(message);
            }
        }
        
        #endregion

        #region ClientEntity ...

        public override void Close()
        {
            if (this.session != null)
            {
                this.link.Close();
                this.session.Close();
                this.isClosed = true;
            }
        }

        #endregion
    }
}
