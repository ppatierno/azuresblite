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
    /// Interface for message client entity
    /// </summary>
    internal interface IMessageClientEntity
    {
        /// <summary>
        /// Close the message client entity
        /// </summary>
        void Close();

        /// <summary>
        /// Message client entity closed status
        /// </summary>
        bool IsClosed { get; }
    }
}
