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
    /// Event sent and received from an event hub
    /// </summary>
    public sealed class EventData : IDisposable
    {
        private bool disposed;

        private Stream bodyStream;

        /// <summary>
        /// System properties for the event
        /// </summary>
        public IDictionary SystemProperties { get; private set; }

        /// <summary>
        /// User properties for the event
        /// </summary>
        public IDictionary Properties { get; private set; }

        /// <summary>
        /// Key used to determine to which partition to send event
        /// </summary>
        public string PartitionKey
        {
            get
            {
                if (this.SystemProperties.Contains("PartitionKey"))
                    return (string)this.SystemProperties["PartitionKey"];
                return default(string);
            }
            set
            {
                this.SystemProperties["PartitionKey"] = value;
            }
        }

        /// <summary>
        /// Gets the offset of the data relative to the Event Hub partition stream
        /// </summary>
        public string Offset
        {
            get
            {
                if (this.SystemProperties.Contains("Offset"))
                    return (string)this.SystemProperties["Offset"];
                return default(string);
            }
            internal set 
            {
                this.SystemProperties["Offset"] = value;
            }
        }

        /// <summary>
        ///  Gets the logical sequence number of the event within the partition stream of the Event Hub
        /// </summary>
        public long SequenceNumber
        {
            get
            {
                if (this.SystemProperties.Contains("SequenceNumber"))
                    return (long)this.SystemProperties["SequenceNumber"];
                return default(long);
            }
            internal set
            {
                this.SystemProperties["SequenceNumber"] = value;
            }
        }

        /// <summary>
        /// Gets or sets the date and time of the sent time in UTC
        /// </summary>
        public DateTime EnqueuedTimeUtc
        {
            get
            {
                if (this.SystemProperties.Contains("EnqueuedTimeUtc"))
                    return (DateTime)this.SystemProperties["EnqueuedTimeUtc"];
                return default(DateTime);
            }
            internal set
            {
                this.SystemProperties["EnqueuedTimeUtc"] = value;
            }
        }

        /// <summary>
        /// Gets or sets the publisher name
        /// </summary>
        internal string Publisher
        {
            get
            {
                if (this.SystemProperties.Contains("Publisher"))
                    return (string)this.SystemProperties["Publisher"];
                return default(string);
            }
            set
            {
                this.SystemProperties["Publisher"] = value;
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public EventData()
        {
            this.SystemProperties = new Hashtable();
            this.Properties = new Hashtable();
            this.bodyStream = null;
        }

        /// <summary>
        /// Constructor with a body from a stream
        /// </summary>
        /// <param name="stream">Stream for the body message</param>
        public EventData(Stream stream)
            : this()
        {
            this.bodyStream = stream;
        }

        /// <summary>
        /// Constructor with a body as bytes
        /// </summary>
        /// <param name="bytes">Bytes for the body message</param>
        public EventData(byte[] bytes)
            : this(new MemoryStream(bytes))
        {
        }

        /// <summary>
        /// Constructor from an AMQP message
        /// </summary>
        /// <param name="amqpMessage">AMQP message</param>
        internal EventData(Message amqpMessage) 
            : this()
        {
            if (amqpMessage == null)
                throw new ArgumentNullException("amqpMessage");

            MessageConverter.AmqpMessageToEventData(amqpMessage, this);

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
        /// Convert current event data object in a AMQP message
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
            MessageConverter.EventDataToAmqpMessage(this, message);
            return message;
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
