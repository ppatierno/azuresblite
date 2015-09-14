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
using ppatierno.AzureSBLite.Utility;
using System;
using System.Text;

namespace ppatierno.AzureSBLite
{
    /// <summary>
    /// SAS token provider
    /// </summary>
    public class SharedAccessSignatureTokenProvider : TokenProvider
    {
        /// <summary>
        /// Key name
        /// </summary>
        internal string KeyName { get; private set; }

        /// <summary>
        /// Shared access key
        /// </summary>
        internal string SharedAccessKey { get; private set; }

        /// <summary>
        /// Shared access signature
        /// </summary>
        internal string ShareAccessSignature { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="keyName">Key name</param>
        /// <param name="sharedAccessKey">Shared access key</param>
        internal SharedAccessSignatureTokenProvider(string keyName, string sharedAccessKey)
        {
            this.KeyName = keyName;
            this.SharedAccessKey = sharedAccessKey;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="shareAccessSignature">Shared access signature</param>
        internal SharedAccessSignatureTokenProvider(string shareAccessSignature)
        {
            this.ShareAccessSignature = shareAccessSignature;
        }

        private static readonly long UtcReference = (new DateTime(1970, 1, 1, 0, 0, 0, 0)).Ticks;


        public static string GetSharedAccessSignature(string keyName, string sharedAccessKey, string resource, TimeSpan tokenTimeToLive)
        {
            // http://msdn.microsoft.com/en-us/library/azure/dn170477.aspx
            // the canonical Uri scheme is http because the token is not amqp specific
            // signature is computed from joined encoded request Uri string and expiry string

#if NETMF
            // needed in .Net Micro Framework to use standard RFC4648 Base64 encoding alphabet
            System.Convert.UseRFC4648Encoding = true;
#endif
            string expiry = ((long)(DateTime.UtcNow - new DateTime(UtcReference, DateTimeKind.Utc) + tokenTimeToLive).TotalSeconds()).ToString();
            string encodedUri = HttpUtility.UrlEncode(resource);

            byte[] hmac = SHA.computeHMAC_SHA256(Encoding.UTF8.GetBytes(sharedAccessKey), Encoding.UTF8.GetBytes(encodedUri + "\n" + expiry));
            string sig = Convert.ToBase64String(hmac);

            return Fx.Format(
                "SharedAccessSignature sr={0}&sig={1}&se={2}&skn={3}",
                encodedUri,
                HttpUtility.UrlEncode(sig),
                HttpUtility.UrlEncode(expiry),
                HttpUtility.UrlEncode(keyName));
        }

        public static string GetPublisherSharedAccessSignature(Uri endpoint, string entityPath, string publisher, string keyName, string key, TimeSpan tokenTimeToLive)
        {
            string publisherPath = Fx.Format("http://{0}/{1}/Publishers/{2}", endpoint.Host, entityPath, publisher);

            return GetSharedAccessSignature(keyName, key, publisherPath, tokenTimeToLive);
        }
    }
}
