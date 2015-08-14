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
using Amqp.Framing;
using ppatierno.AzureSBLite.Messaging.Amqp;
using System;
using System.Collections;
using System.IO;

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Message sent and received from service bus
    /// </summary>
    public sealed class BrokeredMessage : IDisposable
    {
        private bool disposed;

        private Stream bodyStream;

        /// <summary>
        /// Number of deliveries
        /// </summary>
        public int DeliveryCount { get; internal set; }

        /// <summary>
        /// Message time to live
        /// </summary>
        public TimeSpan TimeToLive { get; set; }

        /// <summary>
        /// Date and time of sent in UTC
        /// </summary>
        public DateTime EnqueuedTimeUtc { get; internal set; }

        /// <summary>
        /// Unique number assigned to a message by the Service Bus
        /// </summary>
        public long SequenceNumber { get; internal set; }

        /// <summary>
        /// Date and time in UTC until which the message will be locked
        /// </summary>
        public DateTime LockedUntilUtc { get; internal set; }

        /// <summary>
        /// Partition key for sending a transactional message 
        /// </summary>
        public string PartitionKey { get; set; }

        /// <summary>
        /// Identifier of the message
        /// </summary>
        public string MessageId { get; set; }

        /// <summary>
        /// Identifier of the correlation
        /// </summary>
        public string CorrelationId { get; set; }

        /// <summary>
        /// Type of the content
        /// </summary>
        public string ContentType { get; set; }

        /// <summary>
        /// Message label
        /// </summary>
        public string Label { get; set; }

        /// <summary>
        /// Address to send
        /// </summary>
        public string To { get; set; }

        /// <summary>
        /// Address of the queue to reply to
        /// </summary>
        public string ReplyTo { get; set; }

        /// <summary>
        /// Identifier of the session
        /// </summary>
        public string SessionId { get; set; }

        /// <summary>
        /// Identifier of the session to reply to
        /// </summary>
        public string ReplyToSessionId { get; set; }

        /// <summary>
        /// Date and time in UTC at which the message will be enqueued
        /// </summary>
        public DateTime ScheduledEnqueueTimeUtc { get; set; }

        /// <summary>
        /// Publisher name
        /// </summary>
        internal string Publisher { get; set; }

        /// <summary>
        /// User properties for the event
        /// </summary>
        public IDictionary Properties { get; private set; }

        /// <summary>
        /// Lock token assigned to the message (in peeklock mode)
        /// </summary>
        public Guid LockToken { get; internal set; }

        /// <summary>
        /// Receiver who gets messages from the Service Bus
        /// </summary>
        internal MessageReceiver Receiver { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public BrokeredMessage()
        {
            this.Properties = new Hashtable();
        }

        /// <summary>
        /// Constructor with a body from a stream
        /// </summary>
        /// <param name="stream">Stream for the body message</param>
        public BrokeredMessage(Stream stream)
            : this()
        {
            this.bodyStream = stream;
        }

        /// <summary>
        /// Constructor from an AMQP message
        /// </summary>
        /// <param name="amqpMessage">AMQP message</param>
        internal BrokeredMessage(Message amqpMessage) 
            : this()
        {
            if (amqpMessage == null)
                throw new ArgumentNullException("amqpMessage");

            MessageConverter.AmqpMessageToBrokeredMessage(amqpMessage, this);

            this.bodyStream = (amqpMessage.Body != null) ? new MemoryStream((byte[])amqpMessage.Body) : null;
        }

        /// <summary>
        /// Get body message bytes
        /// </summary>
        /// <returns>Body bytes</returns>
        public byte[] GetBytes()
        {
            if (this.bodyStream == null)
            {
                return new byte[0];
            }
            else
            {
                byte[] buffer = new byte[this.bodyStream.Length];
                this.bodyStream.Read(buffer, 0, buffer.Length);
                return buffer;
            }
        }

        /// <summary>
        /// Complete the receive operation on the message
        /// </summary>
        public void Complete()
        {
            this.Receiver.Complete(this.LockToken);
        }

        /// <summary>
        /// Convert current brokered message object in a AMQP message
        /// </summary>
        /// <returns>AMQP message</returns>
        internal Message ToAmqpMessage()
        {
            Message message = null;
            if (this.bodyStream == null)
            {
                message = new Message();
            }
            else
            {
                byte[] buffer = new byte[this.bodyStream.Length];
                this.bodyStream.Read(buffer, 0, buffer.Length);
                message = new Message()
                {
                    BodySection = new Data() { Binary = buffer }
                };
            }

            if (message.Properties == null)
                message.Properties = new Properties();

            this.MessageId = NewMessageId();

            MessageConverter.BrokeredMessageToAmqpMessage(this, message);
            return message;
        }

        private static string NewMessageId()
        {
            return Guid.NewGuid().ToString();
        }

        public void Dispose()
        {
            this.Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    // TODO : any resources ?
                }
                this.disposed = true;
            }
        }
    }
}
