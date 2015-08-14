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

namespace ppatierno.AzureSBLite
{
    /// <summary>
    /// Base classe for token providers
    /// </summary>
    public abstract class TokenProvider
    {
        /// <summary>
        /// Create a provider for SAS tokens
        /// </summary>
        /// <param name="keyName">Key name</param>
        /// <param name="sharedAccessKey">Shared access key</param>
        /// <returns>SAS token provider instance</returns>
        public static TokenProvider CreateSharedAccessSignatureTokenProvider(string keyName, string sharedAccessKey)
        {
            return new SharedAccessSignatureTokenProvider(keyName, sharedAccessKey);
        }

        /// <summary>
        /// Create a provider for SAS tokens
        /// </summary>
        /// <param name="sharedAccessSignature">Shared access signature</param>
        /// <returns>SAS token provider instance</returns>
        public static TokenProvider CreateSharedAccessSignatureTokenProvider(string sharedAccessSignature)
        {
            return new SharedAccessSignatureTokenProvider(sharedAccessSignature);
        }
    }
}
