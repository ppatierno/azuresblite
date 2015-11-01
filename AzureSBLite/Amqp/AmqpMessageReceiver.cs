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
using Amqp.Types;
using ppatierno.AzureSBLite.Utility;
using System;
using System.Collections;

namespace ppatierno.AzureSBLite.Messaging.Amqp
{
    /// <summary>
    /// Message receiver for AMQP protocol
    /// </summary>
    internal sealed class AmqpMessageReceiver : MessageReceiver
    {
        private static readonly long StartOfEpoch = (new DateTime(1970, 1, 1, 0, 0, 0, 0)).Ticks;

        // AMQP messaging factory
        private AmqpMessagingFactory factory;

        // session and link of AMQP protocol
        private Session session;
        private ReceiverLink link;

        // receive mode
        private ReceiveMode receiveMode;

        // collection with peeked message to complete/abandon with "lock token"
        private IDictionary peekedMessages;

        // entity to connect (AMQP node)
        private string entity;

        /// <summary>
        /// Construcotr
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        /// <param name="entity">Entity path</param>
        internal AmqpMessageReceiver(AmqpMessagingFactory factory, string entity) 
            : this(factory, entity, ReceiveMode.PeekLock)
        {

        }

        /// <summary>
        /// Construcotr
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        /// <param name="entity">Entity path</param>
        /// <param name="receiveMode">Receive mode</param>
        internal AmqpMessageReceiver(AmqpMessagingFactory factory, string entity, ReceiveMode receiveMode)
            : base(factory)
        {
            this.factory = factory;
            this.entity = entity;
            this.receiveMode = receiveMode;

            this.peekedMessages = new Hashtable();
        }

        #region MessageReceiver ...

        public override string Path
        {
            get
            {
                return this.entity;
            }
        }

        internal override EventData ReceiveEventData()
        {
            if (this.factory.Open(this.entity))
            {
                if (this.session == null)
                {
                    this.session = new Session(this.factory.Connection);
                    // no offsets are set
                    if (((this.StartOffset == null) || (this.StartOffset == string.Empty)) && (this.ReceiverStartTime == DateTime.MaxValue))
                    {
                        this.link = new ReceiverLink(this.session, "amqp-receive-link " + this.entity, this.entity);
                    }
                    // existing offsets to set as filters
                    else
                    {
                        Map filters = new Map();

                        if ((this.StartOffset != null) && (this.StartOffset != string.Empty))
                        {
                            filters.Add(new Symbol("apache.org:selector-filter:string"),
                                        new DescribedValue(
                                            new Symbol("apache.org:selector-filter:string"),
                                            "amqp.annotation.x-opt-offset > '" + this.StartOffset + "'"));
                        }

                        if (this.ReceiverStartTime != DateTime.MaxValue)
                        {
                            string totalMilliseconds = ((long)(this.ReceiverStartTime.ToUniversalTime() - new DateTime(StartOfEpoch, DateTimeKind.Utc)).TotalMilliseconds()).ToString();

                            filters.Add(new Symbol("apache.org:selector-filter:string"),
                                        new DescribedValue(
                                            new Symbol("apache.org:selector-filter:string"),
                                            "amqp.annotation.x-opt-enqueuedtimeutc > " + totalMilliseconds + ""));
                        }

                        this.link = new ReceiverLink(this.session, "amqp-receive-link " + this.entity,
                                        new global::Amqp.Framing.Source()
                                        {
                                            Address = this.entity,
                                            FilterSet = filters
                                        }, null);
                    }
                }

                Message message = this.link.Receive();

                if (message != null)
                {
                    this.link.Accept(message);
                    return new EventData(message);
                }
                
                return null;
            }

            return null;
        }

        public override BrokeredMessage Receive()
        {
            if (this.factory.Open(this.entity))
            {
                if (this.session == null)
                {
                    this.session = new Session(this.factory.Connection);
                    this.link = new ReceiverLink(this.session, "amqp-receive-link " + this.entity, this.entity);
                }

                this.link.SetCredit(1, false);
                Message message = this.link.Receive();

                if (message != null)
                {
                    BrokeredMessage brokeredMessage = new BrokeredMessage(message);

                    // accept message if receive and delete mode
                    if (this.receiveMode == ReceiveMode.ReceiveAndDelete)
                        this.link.Accept(message);
                    else
                    {
                        // get "lock token" and add message to peeked messages collection
                        // to enable complete or abandon in the future
                        brokeredMessage.LockToken = new Guid(message.DeliveryTag);
                        brokeredMessage.Receiver = this;
                        this.peekedMessages.Add(brokeredMessage.LockToken, message);
                    }
                    return brokeredMessage;
                }

                return null;
            }

            return null;
        }

        private void Outcome(Guid lockToken, bool accept)
        {
            if (this.peekedMessages.Contains(lockToken))
            {
                if (accept)
                    this.link.Accept((Message)this.peekedMessages[lockToken]);
                else
                    this.link.Release((Message)this.peekedMessages[lockToken]);

                this.peekedMessages.Remove(lockToken);
            }
        }

        public override void Complete(Guid lockToken)
        {
            this.Outcome(lockToken, true);
        }

        public override void Abandon(Guid lockToken)
        {
            this.Outcome(lockToken, false);
        }

        public override void OnMessage(OnMessageAction callback, OnMessageOptions options)
        {
            if (this.factory.Open(this.entity))
            {
                if (this.session == null)
                {
                    this.session = new Session(this.factory.Connection);
                    this.link = new ReceiverLink(this.session, "amqp-receive-link " + this.entity, this.entity);
                }

                // start the message pump
                this.link.Start(1,
                    (r, m) =>
                    {
                        if (m != null)
                        {
                            BrokeredMessage brokeredMessage = new BrokeredMessage(m);
                            callback(brokeredMessage);

                            //  if autocomplete requested
                            if (options.AutoComplete)
                                r.Accept(m);
                        }
                    });
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
