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

using System.Collections;
using Amqp.Framing;
using System;
using Amqp.Types;
using Amqp;
using ppatierno.AzureSBLite.Utility;

namespace ppatierno.AzureSBLite.Messaging.Amqp
{
    /// <summary>
    /// Execute message converter operations for AMQP protocol
    /// </summary>
    internal static class MessageConverter
    {
        #region Constants ...

        // AMQP message annotations keys
        public const string PARTITION_KEY_NAME = "x-opt-partition-key";
        public const string OFFSET_NAME = "x-opt-offset";
        public const string SEQUENCE_NUMBER_NAME = "x-opt-sequence-number";
        public const string ENQUEUED_TIME_UTC_NAME = "x-opt-enqueued-time";
        public const string PUBLISHER_NAME = "x-opt-publisher";
        public const string LOCKED_UNTIL_NAME = "x-opt-locked-until";
        public const string SCHEDULED_ENQUEUE_TIME_UTC = "x-opt-scheduled-enqueue-time";

        #endregion

        private static IDictionary typeMap;

        static MessageConverter()
        {
            typeMap = new Hashtable();
            typeMap.Add(typeof(byte), PropertyValueType.Byte);
            typeMap.Add(typeof(sbyte), PropertyValueType.SByte);
            typeMap.Add(typeof(char), PropertyValueType.Char);
            typeMap.Add(typeof(short), PropertyValueType.Int16);
            typeMap.Add(typeof(ushort), PropertyValueType.UInt16);
            typeMap.Add(typeof(int), PropertyValueType.Int32);
            typeMap.Add(typeof(uint), PropertyValueType.UInt32);
            typeMap.Add(typeof(long), PropertyValueType.Int64);
            typeMap.Add(typeof(ulong), PropertyValueType.UInt64);
            typeMap.Add(typeof(float), PropertyValueType.Single);
            typeMap.Add(typeof(double), PropertyValueType.Double);
            // .Net Micro Framework doesn't support "decimal" type
#if !NETMF
            typeMap.Add(typeof(decimal), PropertyValueType.Decimal);
#endif
            typeMap.Add(typeof(bool), PropertyValueType.Boolean);
            typeMap.Add(typeof(Guid), PropertyValueType.Guid);
            typeMap.Add(typeof(string), PropertyValueType.String);
            typeMap.Add(typeof(Uri), PropertyValueType.Uri);
            typeMap.Add(typeof(DateTime), PropertyValueType.DateTime);
            typeMap.Add(typeof(TimeSpan), PropertyValueType.TimeSpan);
        }

        /// <summary>
        /// Execute mapping from EventData structure to AMQP message structure
        /// </summary>
        /// <param name="eventData">EventData instance</param>
        /// <param name="message">AMQP message instance</param>
        internal static void EventDataToAmqpMessage(EventData eventData, Message message)
        {
            if ((eventData.Publisher != null) && (eventData.Publisher != string.Empty))
            {
                if (message.MessageAnnotations == null)
                    message.MessageAnnotations = new MessageAnnotations();
                message.MessageAnnotations[new Symbol(PUBLISHER_NAME)] = eventData.Publisher;
            }
            if (eventData.PartitionKey != null)
            {
                if (message.MessageAnnotations == null)
                    message.MessageAnnotations = new MessageAnnotations();
                message.MessageAnnotations[new Symbol(PARTITION_KEY_NAME)] = eventData.PartitionKey;
            }
            if (eventData.Properties.Count > 0)
            {
                if (message.ApplicationProperties == null)
                    message.ApplicationProperties = new ApplicationProperties();

                foreach (DictionaryEntry pair in eventData.Properties)
                {
                    object amqpObject = null;
                    if (NetObjectToAmqpObject(pair.Value, out amqpObject))
                    {
                        message.ApplicationProperties[pair.Key] = amqpObject;
                    }
                }
            }
        }

        /// <summary>
        /// Execute mapping from AMQP message structure to EventData structure
        /// </summary>
        /// <param name="message">AMQP message instance</param>
        /// <param name="eventData">EventData instance</param>
        internal static void AmqpMessageToEventData(Message message, EventData eventData)
        {
            if (message.MessageAnnotations != null)
            {
                object annotation = null;
                annotation = message.MessageAnnotations[new Symbol(PUBLISHER_NAME)];
                if (annotation != null)
                    eventData.Publisher = (string)annotation;

                annotation = message.MessageAnnotations[new Symbol(PARTITION_KEY_NAME)];
                if (annotation != null)
                    eventData.PartitionKey = (string)annotation;

                annotation = message.MessageAnnotations[new Symbol(ENQUEUED_TIME_UTC_NAME)];
                if (annotation != null)
                    eventData.EnqueuedTimeUtc = (DateTime)annotation;

                annotation = message.MessageAnnotations[new Symbol(SEQUENCE_NUMBER_NAME)];
                if (annotation != null)
                    eventData.SequenceNumber = (long)annotation;

                annotation = message.MessageAnnotations[new Symbol(OFFSET_NAME)];
                if (annotation != null)
                    eventData.Offset = (string)annotation;
            }

            if (message.ApplicationProperties != null)
            {
                foreach (var key in message.ApplicationProperties.Map.Keys)
                {
                    object netObject = null;
                    if (AqmpObjectToNetObject(message.ApplicationProperties.Map[key], out netObject))
                    {
                        eventData.Properties[key] = netObject;
                    }
                }
            }
        }

        /// <summary>
        /// Execute mapping from BrokeredMessage structure to AMQP message structure
        /// </summary>
        /// <param name="brokeredMessage">Brokered message instance</param>
        /// <param name="message">AMQP message instance</param>
        internal static void BrokeredMessageToAmqpMessage(BrokeredMessage brokeredMessage, Message message)
        {
            if (brokeredMessage.DeliveryCount != 0)
            {
                message.Header.DeliveryCount = (uint)brokeredMessage.DeliveryCount;
            }
            if (brokeredMessage.TimeToLive != TimeSpan.Zero)
            {
                message.Header.Ttl = (uint)brokeredMessage.TimeToLive.TotalMilliseconds();
            }
            if (brokeredMessage.EnqueuedTimeUtc != DateTime.MinValue)
            {
                message.MessageAnnotations[new Symbol(ENQUEUED_TIME_UTC_NAME)] = brokeredMessage.EnqueuedTimeUtc;
            }
            if (brokeredMessage.SequenceNumber != 0)
            {
                message.MessageAnnotations[new Symbol(SEQUENCE_NUMBER_NAME)] = brokeredMessage.SequenceNumber;
            }
            if (brokeredMessage.LockedUntilUtc != DateTime.MinValue)
            {
                message.MessageAnnotations[new Symbol(LOCKED_UNTIL_NAME)] = brokeredMessage.LockedUntilUtc;
            }
            if ((brokeredMessage.Publisher != null) && (brokeredMessage.Publisher != string.Empty))
            {
                message.MessageAnnotations[new Symbol(PUBLISHER_NAME)] = brokeredMessage.Publisher;
            }
            if ((brokeredMessage.PartitionKey != null) && (brokeredMessage.PartitionKey != string.Empty))
            {
                message.MessageAnnotations[new Symbol(PARTITION_KEY_NAME)] = brokeredMessage.PartitionKey;
            }
            if ((brokeredMessage.MessageId != null) && (brokeredMessage.MessageId != string.Empty))
            {
                message.Properties.MessageId = brokeredMessage.MessageId;
            }
            if ((brokeredMessage.ContentType != null) && (brokeredMessage.ContentType != string.Empty))
            {
                message.Properties.ContentType = new Symbol(brokeredMessage.ContentType);
            }
            if ((brokeredMessage.CorrelationId != null) && (brokeredMessage.CorrelationId != string.Empty))
            {
                message.Properties.CorrelationId = brokeredMessage.CorrelationId;
            }
            if (brokeredMessage.DeliveryCount != 0)
            {
                message.Header.DeliveryCount = (uint)brokeredMessage.DeliveryCount;
            }
            if ((brokeredMessage.Label != null) && (brokeredMessage.Label != string.Empty))
            {
                message.Properties.Subject = brokeredMessage.Label;
            }
            if ((brokeredMessage.ReplyTo != null) && (brokeredMessage.ReplyTo != string.Empty))
            {
                message.Properties.ReplyTo = brokeredMessage.ReplyTo;
            }
            if ((brokeredMessage.ReplyToSessionId != null) && (brokeredMessage.ReplyToSessionId != string.Empty))
            {
                message.Properties.ReplyToGroupId = brokeredMessage.ReplyToSessionId;
            }
            if (brokeredMessage.ScheduledEnqueueTimeUtc != DateTime.MinValue)
            {
                message.MessageAnnotations[new Symbol(SCHEDULED_ENQUEUE_TIME_UTC)] = brokeredMessage.ScheduledEnqueueTimeUtc;
            }
            if ((brokeredMessage.SessionId != null) && (brokeredMessage.SessionId != string.Empty))
            {
                message.Properties.GroupId = brokeredMessage.SessionId;
            }
            if ((brokeredMessage.To != null) && (brokeredMessage.To != string.Empty))
            {
                message.Properties.To = brokeredMessage.To;
            }

            if (brokeredMessage.Properties.Count > 0)
            {
                if (message.ApplicationProperties == null)
                    message.ApplicationProperties = new ApplicationProperties();

                foreach (DictionaryEntry pair in brokeredMessage.Properties)
                {
                    object amqpObject = null;
                    if (NetObjectToAmqpObject(pair.Value, out amqpObject))
                    {
                        message.ApplicationProperties[pair.Key] = amqpObject;
                    }
                }
            }
        }

        /// <summary>
        /// Execute mapping from AMQP message structure to BrokeredMessage structure
        /// </summary>
        /// <param name="message">AMQP message instance</param>
        /// <param name="brokeredMessage">Brokered message instance</param>
        internal static void AmqpMessageToBrokeredMessage(Message message, BrokeredMessage brokeredMessage)
        {
            if (message.Header.Ttl != 0)
            {
                brokeredMessage.TimeToLive = TimeSpanExtension.FromMilliseconds(message.Header.Ttl);
            }
            if ((message.Properties.MessageId != null) && (message.Properties.MessageId != string.Empty))
            {
                brokeredMessage.MessageId = message.Properties.MessageId;
            }
            if ((message.Properties.CorrelationId != null) && (message.Properties.CorrelationId != string.Empty))
            {
                brokeredMessage.CorrelationId = message.Properties.CorrelationId;
            }
            if (message.Properties.ContentType != null)
            {
                brokeredMessage.ContentType = message.Properties.ContentType.ToString();
            }
            if ((message.Properties.Subject != null) && (message.Properties.Subject != string.Empty))
            {
                brokeredMessage.Label = message.Properties.Subject;
            }
            if ((message.Properties.To != null) && (message.Properties.To != string.Empty))
            {
                brokeredMessage.To = message.Properties.To;
            }
            if ((message.Properties.ReplyTo != null) && (message.Properties.ReplyTo != string.Empty))
            {
                brokeredMessage.ReplyTo = message.Properties.ReplyTo;
            }
            if ((message.Properties.GroupId != null) && (message.Properties.GroupId != string.Empty))
            {
                brokeredMessage.SessionId = message.Properties.GroupId;
            }
            if ((message.Properties.ReplyToGroupId != null) && (message.Properties.ReplyToGroupId != string.Empty))
            {
                brokeredMessage.ReplyToSessionId = message.Properties.ReplyToGroupId;
            }

            if (message.MessageAnnotations != null)
            {
                object annotation = null;
                annotation = message.MessageAnnotations[new Symbol(PUBLISHER_NAME)];
                if (annotation != null)
                    brokeredMessage.Publisher = (string)annotation;

                annotation = message.MessageAnnotations[new Symbol(PARTITION_KEY_NAME)];
                if (annotation != null)
                    brokeredMessage.PartitionKey = (string)annotation;

                annotation = message.MessageAnnotations[new Symbol(SCHEDULED_ENQUEUE_TIME_UTC)];
                if (annotation != null)
                    brokeredMessage.ScheduledEnqueueTimeUtc = (DateTime)annotation;

                annotation = message.MessageAnnotations[new Symbol(ENQUEUED_TIME_UTC_NAME)];
                if (annotation != null)
                    brokeredMessage.EnqueuedTimeUtc = (DateTime)annotation;

                annotation = message.MessageAnnotations[new Symbol(SEQUENCE_NUMBER_NAME)];
                if (annotation != null)
                    brokeredMessage.SequenceNumber = (long)annotation;

                annotation = message.MessageAnnotations[new Symbol(LOCKED_UNTIL_NAME)];
                if (annotation != null)
                    brokeredMessage.LockedUntilUtc = (DateTime)annotation;
            }

            if (message.ApplicationProperties != null)
            {
                foreach (var key in message.ApplicationProperties.Map.Keys)
                {
                    object netObject = null;
                    if (AqmpObjectToNetObject(message.ApplicationProperties.Map[key], out netObject))
                    {
                        brokeredMessage.Properties[key] = netObject;
                    }
                }
            }
        }

        /// <summary>
        /// Execute mapping from .Net object to AMQP object
        /// </summary>
        /// <param name="netObject">.Net object</param>
        /// <param name="amqpObject">AMQP object</param>
        /// <returns>Object converted (or not)</returns>
        internal static bool NetObjectToAmqpObject(object netObject, out object amqpObject)
        {
            amqpObject = null;
            if (netObject == null)
            {
                return false;
            }
            switch (GetTypeId(netObject))
            {
                case PropertyValueType.Byte:
                case PropertyValueType.SByte:
                case PropertyValueType.Char:
                case PropertyValueType.Int16:
                case PropertyValueType.UInt16:
                case PropertyValueType.Int32:
                case PropertyValueType.UInt32:
                case PropertyValueType.Int64:
                case PropertyValueType.UInt64:
                case PropertyValueType.Single:
                case PropertyValueType.Double:
                case PropertyValueType.Decimal:
                case PropertyValueType.Boolean:
                case PropertyValueType.Guid:
                case PropertyValueType.String:
                case PropertyValueType.DateTime:
                    amqpObject = netObject;
                    break;
            }
            return (amqpObject != null);
        }

        /// <summary>
        /// Execute mapping from AMQP object to .Net object
        /// </summary>
        /// <param name="amqpObject">AMQP object</param>
        /// <param name="netObject">.Net object</param>
        /// <returns>Object converted (or not)</returns>
        internal static bool AqmpObjectToNetObject(object amqpObject, out object netObject)
        {
            netObject = null;
            if (amqpObject == null)
            {
                return false;
            }
            switch (GetTypeId(amqpObject))
            {
                case PropertyValueType.Byte:
                case PropertyValueType.SByte:
                case PropertyValueType.Char:
                case PropertyValueType.Int16:
                case PropertyValueType.UInt16:
                case PropertyValueType.Int32:
                case PropertyValueType.UInt32:
                case PropertyValueType.Int64:
                case PropertyValueType.UInt64:
                case PropertyValueType.Single:
                case PropertyValueType.Double:
                case PropertyValueType.Decimal:
                case PropertyValueType.Boolean:
                case PropertyValueType.Guid:
                case PropertyValueType.String:
                case PropertyValueType.DateTime:
                    netObject = amqpObject;
                    break;
            }
            return (netObject != null);
        }

        /// <summary>
        /// Get property value type from .Net object
        /// </summary>
        /// <param name="value">.Net object</param>
        /// <returns>Property value type</returns>
        private static PropertyValueType GetTypeId(object value)
        {
            PropertyValueType type;
            if (value == null)
            {
                return PropertyValueType.Null;
            }
            if (typeMap.Contains(value.GetType()))
            {
                type = (PropertyValueType)typeMap[value.GetType()];
                return type;
            }
            return PropertyValueType.Unknown;
        }
    }

    internal enum PropertyValueType
    {
        Null,
        Byte,
        SByte,
        Char,
        Int16,
        UInt16,
        Int32,
        UInt32,
        Int64,
        UInt64,
        Single,
        Double,
        Decimal,
        Boolean,
        Guid,
        String,
        Uri,
        DateTime,
        DateTimeOffset,
        TimeSpan,
        Stream,
        Unknown
    }
}
