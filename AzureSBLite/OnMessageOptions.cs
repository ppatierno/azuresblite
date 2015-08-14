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

namespace ppatierno.AzureSBLite.Messaging
{
    /// <summary>
    /// Options for the message pump
    /// </summary>
    public sealed class OnMessageOptions
    {
        /// <summary>
        /// Message pump call complete or not when callback completed processing
        /// </summary>
        public bool AutoComplete { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public OnMessageOptions()
        {
            this.AutoComplete = true;
        }
    }
}
