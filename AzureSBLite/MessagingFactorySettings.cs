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

using ppatierno.AzureSBLite.Messaging.Amqp;

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Messaging factory settings
    /// </summary>
    public class MessagingFactorySettings
    {
        /// <summary>
        /// Transport settings for AMQP protocol
        /// </summary>
        public AmqpTransportSettings AmqpTransportSettings { get; set; }

        /// <summary>
        /// Token provider of the factory settings
        /// </summary>
        public TokenProvider TokenProvider { get; set; }

        /// <summary>
        /// Transport type
        /// </summary>
        public TransportType TransportType { get; set; }

        /// <summary>
        /// Construtor
        /// </summary>
        public MessagingFactorySettings()
        {
            this.TransportType = TransportType.Amqp;
            this.TokenProvider = null;
            this.AmqpTransportSettings = null;
        }
    }
}
