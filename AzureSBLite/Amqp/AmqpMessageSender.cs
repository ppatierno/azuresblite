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

namespace ppatierno.AzureSBLite.Messaging.Amqp
{
    /// <summary>
    /// Message sender for AMQP protocol
    /// </summary>
    internal sealed class AmqpMessageSender : MessageSender
    {
        // AMQP messaging factory
        private AmqpMessagingFactory factory;

        // session and link of AMQP protocol
        private Session session;
        private SenderLink link;

        // entity to connect (AMQP node)
        private string entity;

        /// <summary>
        /// Construcotr
        /// </summary>
        /// <param name="factory">Messaging factory instance</param>
        /// <param name="entity">Path to event hub entity</param>
        internal AmqpMessageSender(AmqpMessagingFactory factory, string entity) 
            : base(factory)
        {
            this.factory = factory;
            this.entity = entity;
        }

        #region MessageSender ...

        internal override void SendEventData(EventData data)
        {
            if (this.factory.OpenConnection())
            {
                bool canSend = true;

                if (this.session == null)
                {
                    if (this.factory.TransportSettings.TokenProvider.GetType() == typeof(SharedAccessSignatureTokenProvider))
                    {
                        SharedAccessSignatureTokenProvider tokenProvider = (SharedAccessSignatureTokenProvider)this.factory.TransportSettings.TokenProvider;

                        if ((tokenProvider.ShareAccessSignature != null) && (tokenProvider.ShareAccessSignature != string.Empty))
                        {
                            canSend = PutCbsToken(tokenProvider.ShareAccessSignature);
                        }
                    }

                    if (canSend)
                    {
                        this.session = new Session(this.factory.Connection);
                        this.link = new SenderLink(this.session, "amqp-send-link " + this.entity, this.entity);
                    }
                }

                if (canSend)
                {
                    Message message = data.ToAmqpMessage();
                    this.link.Send(message);
                }
            }
        }

        public override void Send(BrokeredMessage brokeredMessage)
        {
            if (this.factory.OpenConnection())
            {
                if (this.session == null)
                {
                    this.session = new Session(this.factory.Connection);
                    this.link = new SenderLink(this.session, "amqp-send-link " + this.entity, entity);
                }

                Message message = brokeredMessage.ToAmqpMessage();
                this.link.Send(message);
            }
        }

        /// <summary>
        /// Send Claim Based Security (CBS) token
        /// </summary>
        /// <param name="shareAccessSignature">Shared access signature (token) to send</param>
        private bool PutCbsToken(string shareAccessSignature)
        {
            bool result = true;
            Session session = new Session(this.factory.Connection);

            string cbsClientAddress = "cbs-receiver/123";
            var cbsSender = new SenderLink(session, "cbs-sender", "$cbs");
            var cbsReceiver = new ReceiverLink(session, cbsClientAddress, "$cbs");

            // construct the put-token message
            var request = new Message(shareAccessSignature);
            request.Properties = new Properties();
            request.Properties.MessageId = "1";
            request.Properties.ReplyTo = cbsClientAddress;
            request.ApplicationProperties = new ApplicationProperties();
            request.ApplicationProperties["operation"] = "put-token";
            request.ApplicationProperties["type"] = "servicebus.windows.net:sastoken";
            request.ApplicationProperties["name"] = Fx.Format("amqp://{0}/{1}", this.factory.Address.Host, this.entity);
            cbsSender.Send(request);

            // receive the response
            var response = cbsReceiver.Receive();
            if (response == null || response.Properties == null || response.ApplicationProperties == null)
            {
                result = false;
            }
            else
            {
                int statusCode = (int)response.ApplicationProperties["status-code"];
                //if (statusCode != (int)HttpStatusCode.Accepted && statusCode != (int)HttpStatusCode.OK)
                if (statusCode != (int)202 && statusCode != (int)200)
                {
                    result = false;
                }
            }

            // the sender/receiver may be kept open for refreshing tokens
            cbsSender.Close();
            cbsReceiver.Close();
            session.Close();

            return result;
        }

        #endregion

        #region ClientEntity ...

        public override void Close()
        {
            if (this.session != null)
            {
                this.link.Close();
                this.session.Close();
                this.isClosed = true;
            }
        }

        #endregion
    }
}
