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
    /// Transport settings for AMQP connection
    /// </summary>
    public sealed class AmqpTransportSettings
    {
        #region Constants ...

        // schemas and ports for AMQP protocol
        internal const string AMQP_SCHEMA = "amqp";
        internal const string AMQPS_SCHEMA = "amqps";
        internal const int AMQP_PORT = 5672;
        internal const int AMQPS_PORT = 5671;

        #endregion

        /// <summary>
        /// AMQP port
        /// </summary>
        internal int Port { get; set; }

        /// <summary>
        /// Token provider
        /// </summary>
        internal TokenProvider TokenProvider { get; set; }
    }
}
