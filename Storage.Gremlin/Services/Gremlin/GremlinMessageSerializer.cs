/*
 * Sidub Platform - Storage - Gremlin
 * Copyright (C) 2024 Sidub Inc.
 * All rights reserved.
 *
 * This file is part of Sidub Platform - Storage - Gremlin (the "Product").
 *
 * The Product is dual-licensed under:
 * 1. The GNU Affero General Public License version 3 (AGPLv3)
 * 2. Sidub Inc.'s Proprietary Software License Agreement (PSLA)
 *
 * You may choose to use, redistribute, and/or modify the Product under
 * the terms of either license.
 *
 * The Product is provided "AS IS" and "AS AVAILABLE," without any
 * warranties or conditions of any kind, either express or implied, including
 * but not limited to implied warranties or conditions of merchantability and
 * fitness for a particular purpose. See the applicable license for more
 * details.
 *
 * See the LICENSE.txt file for detailed license terms and conditions or
 * visit https://sidub.ca/licensing for a copy of the license texts.
 */

#region Imports

using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO;
using Gremlin.Net.Structure.IO.GraphSON;
using Sidub.Platform.Core.Serializers;
using Sidub.Platform.Core.Serializers.Json.Converters;
using System.Text;
using System.Text.Json;

#endregion

namespace Sidub.Platform.Storage.Services.Gremlin
{

    /// <summary>
    /// Represents a message serializer for Gremlin requests and responses.
    /// </summary>
    public class GremlinMessageSerializer : IMessageSerializer
    {

        #region Member variables

        private readonly string _mimeType = SerializationTokens.GraphSON2MimeType;
        private readonly GraphSONReader _graphSONReader;
        private readonly GraphSONWriter _graphSONWriter;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="GremlinMessageSerializer"/> class.
        /// </summary>
        public GremlinMessageSerializer()
        {
            _graphSONReader = new GraphSON2Reader();
            _graphSONWriter = new GraphSON2Writer();
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Serializes a request message to a byte array.
        /// </summary>
        /// <param name="requestMessage">The request message to serialize.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the serialized message as a byte array.</returns>
        public virtual Task<byte[]> SerializeMessageAsync(RequestMessage requestMessage,
            CancellationToken cancellationToken = default)
        {
            if (cancellationToken.CanBeCanceled) cancellationToken.ThrowIfCancellationRequested();
            var graphSONMessage = _graphSONWriter.WriteObject(requestMessage);
            return Task.FromResult(Encoding.UTF8.GetBytes(MessageWithHeader(graphSONMessage)));
        }

        /// <summary>
        /// Deserializes a byte array to a response message.
        /// </summary>
        /// <param name="message">The byte array to deserialize.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task representing the asynchronous operation. The task result contains the deserialized response message.</returns>
        public virtual Task<ResponseMessage<List<object>>?> DeserializeMessageAsync(byte[] message,
            CancellationToken cancellationToken = default)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));
            if (cancellationToken.CanBeCanceled) cancellationToken.ThrowIfCancellationRequested();
            if (message.Length == 0) return Task.FromResult<ResponseMessage<List<object>>?>(null);

            var messageSerializerOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };

            var dataSerializerOptions = SerializerOptions.Default(SerializationLanguageType.Json);

            var serializerOptions = GetJsonSerializerOptions();
            serializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;

            var reader = new Utf8JsonReader(message);
            var responseMessage = JsonSerializer.Deserialize<ResponseMessage<List<Dictionary<string, object?>>>>(ref reader, serializerOptions);
            if (responseMessage == null) return Task.FromResult<ResponseMessage<List<object>>?>(null);

            var resultWrapper = new ResponseResult<List<object>>(responseMessage.Result?.Data?.ToList<object>(), responseMessage.Result?.Meta);

            var responseWrapper = new ResponseMessage<List<object>>(responseMessage.RequestId, responseMessage.Status, resultWrapper);

            return Task.FromResult<ResponseMessage<List<object>>?>(responseWrapper);
        }

        /// <summary>
        /// Gets the JSON serializer options.
        /// </summary>
        /// <returns>The JSON serializer options.</returns>
        public JsonSerializerOptions GetJsonSerializerOptions()
        {
            var jsonSerializerOptions = new JsonSerializerOptions();
            var dictionaryConverter = new DictionaryStringObjectJsonConverter();

            jsonSerializerOptions.Converters.Add(dictionaryConverter);

            return jsonSerializerOptions;
        }

        #endregion

        #region Private methods

        /// <summary>
        /// Adds the message header to the message content.
        /// </summary>
        /// <param name="messageContent">The message content.</param>
        /// <returns>The message with the header.</returns>
        private string MessageWithHeader(string messageContent)
        {
            return $"{(char)_mimeType.Length}{_mimeType}{messageContent}";
        }

        #endregion

    }

}
