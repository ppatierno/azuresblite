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
using System.Collections;
using System.Text.RegularExpressions;
using ppatierno.AzureSBLite.Messaging;
using System.Text;
using Amqp;

namespace ppatierno.AzureSBLite
{
    /// <summary>
    /// Builder for connection string to the Service Bus
    /// </summary>
    public class ServiceBusConnectionStringBuilder
    {
        #region Constants ...

        // connection string parameters
        private const string ENDPOINT = "Endpoint";
        private const string SHARED_ACCESS_KEY_NAME = "SharedAccessKeyName";
        private const string SHARED_ACCESS_KEY = "SharedAccessKey";
        private const string SHARED_ACCESS_SIGNATURE = "SharedAccessSignature";
        private const string ENTITY_PATH = "EntityPath";
        private const string PUBLISHER = "Publisher";
        private const string TRANSPORT_TYPE = "TransportType";

        #endregion

        /// <summary>
        /// Service Bus namespace endpoint
        /// </summary>
        public Uri Endpoint { get; private set; }

        /// <summary>
        /// Shared access key
        /// </summary>
        public string SharedAccessKey { get; set; }
        
        /// <summary>
        /// Shared access key name
        /// </summary>
        public string SharedAccessKeyName { get; set; }
        
        /// <summary>
        /// Shared access signature
        /// </summary>
        public string SharedAccessSignature { get; set; }

        /// <summary>
        /// Publisher identifier
        /// </summary>
        public string Publisher { get; set; }

        /// <summary>
        /// Entity path on Service Bus
        /// </summary>
        public string EntityPath { get; set; }

        /// <summary>
        /// Transport type
        /// </summary>
        public TransportType TransportType { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ServiceBusConnectionStringBuilder()
        {
        }

        /// <summary>
        /// Cnstructor
        /// </summary>
        /// <param name="connectionString">Connection string to the Service Bus</param>
        public ServiceBusConnectionStringBuilder(string connectionString)
        {
            /*
#if !NETMF
            string[] parameters = Regex.Split(";" + connectionString, ";(Endpoint|SharedAccessKeyName|SharedAccessKey|SharedAccessSignature|Publisher|EntityPath|TransportType)=", RegexOptions.IgnoreCase);
#else
            Regex regex = new Regex(";(Endpoint|SharedAccessKeyName|SharedAccessKey|SharedAccessSignature|Publisher|EntityPath|TransportType)=", RegexOptions.IgnoreCase);
            string[] parameters = this.Split(regex, ";" + connectionString);
#endif

            if (parameters[0] != String.Empty)
                throw new ArgumentException("Wrong parameter", "connectionString");

            IDictionary connectionStringParams = new Hashtable();

            for (int i = 1; i < parameters.Length; i++)
            {
                connectionStringParams[parameters[i]] = parameters[i + 1];
                i++;
            }
            */

            Regex regex = new Regex("([^=;]+)=([^;]+)");

            IDictionary connectionStringParams = new Hashtable();

            MatchCollection matches = regex.Matches(connectionString);
            foreach (Match match in matches)
            {
                connectionStringParams[match.Groups[1].Value] = match.Groups[2].Value;
            }

            this.Endpoint = new Uri((string)connectionStringParams[ENDPOINT]);
            this.SharedAccessKeyName = (string)connectionStringParams[SHARED_ACCESS_KEY_NAME];
            this.SharedAccessKey = (string)connectionStringParams[SHARED_ACCESS_KEY];
            this.SharedAccessSignature = (string)connectionStringParams[SHARED_ACCESS_SIGNATURE];
            this.EntityPath = (string)connectionStringParams[ENTITY_PATH];
            this.Publisher = (string)connectionStringParams[PUBLISHER];
            this.TransportType = TransportType.Amqp;
        }

        public static string CreateUsingSharedAccessSignature(Uri endpoint, string entityPath, string publisher, string sharedAccessSignature)
        {
            ServiceBusConnectionStringBuilder builder = new ServiceBusConnectionStringBuilder
            {
                Endpoint = endpoint,
                EntityPath = entityPath,
                Publisher = publisher,
                SharedAccessSignature = sharedAccessSignature
            };
            return builder.ToString();
        }

        public override string ToString()
        {
            StringBuilder builder = new StringBuilder();
            string separator = ";";

            if (this.Endpoint != null)
            {
                builder.Append(Fx.Format("{0}={1}", ENDPOINT, this.Endpoint.AbsoluteUri));
            }
            if ((this.SharedAccessKeyName != null) && (this.SharedAccessKeyName != string.Empty))
            {
                builder.Append(separator);
                builder.Append(Fx.Format("{0}={1}", SHARED_ACCESS_KEY_NAME, this.SharedAccessKeyName));
            }
            if ((this.SharedAccessKey != null) && (this.SharedAccessKey != string.Empty))
            {
                builder.Append(separator);
                builder.Append(Fx.Format("{0}={1}", SHARED_ACCESS_KEY, this.SharedAccessKey));
            }
            if ((this.SharedAccessSignature != null) && (this.SharedAccessSignature != string.Empty))
            {
                builder.Append(separator);
                builder.Append(Fx.Format("{0}={1}", SHARED_ACCESS_SIGNATURE, this.SharedAccessSignature));
            }
            if ((this.EntityPath != null) && (this.EntityPath != string.Empty))
            {
                builder.Append(separator);
                builder.Append(Fx.Format("{0}={1}", ENTITY_PATH, this.EntityPath));
            }
            if ((this.Publisher != null) && (this.Publisher != string.Empty))
            {
                builder.Append(separator);
                builder.Append(Fx.Format("{0}={1}", PUBLISHER, this.Publisher));
            }
            
            builder.Append(separator);
            builder.Append(Fx.Format("{0}={1}", TRANSPORT_TYPE, this.TransportType));
            
            return builder.ToString();
        }

        /*
        private string[] Split(Regex regex, string input)
        {
            Match match = regex.Match(input);
            if (!match.Success)
            {
                return new string[] { input };
            }
            ArrayList list = new ArrayList();

            int startIndex = 0;
            do
            {
                list.Add(input.Substring(startIndex, match.Index - startIndex));
                startIndex = match.Index + match.Length;
                for (int i = 1; i < match.Groups.Count; i++)
                {
                    //if (match.IsMatched(i))
                    {
                        list.Add(match.Groups[i].ToString());
                    }
                }
                match = match.NextMatch();

            } while (match.Success);

            list.Add(input.Substring(startIndex, input.Length - startIndex));

            string[] result = new string[list.Count];
            list.CopyTo(result);
            return result;
        }
        */
    }
}
