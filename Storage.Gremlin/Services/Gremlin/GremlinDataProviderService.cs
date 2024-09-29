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
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Sidub.Platform.Authentication.Services;
using Sidub.Platform.Core;
using Sidub.Platform.Core.Services;
using System.Net.WebSockets;

#endregion

namespace Sidub.Platform.Storage.Services.Gremlin
{

    /// <summary>
    /// Data providersService for Gremlin graph data sources using <see cref="GremlinClient"/>.
    /// </summary>
    public class GremlinDataProviderService : IDataProviderService<GremlinClient>
    {

        #region Member variables

        private readonly IAuthenticationService _authenticationService;
        private readonly IServiceRegistry _metadataService;
        private readonly ILogger<GremlinDataProviderService> _logger;
        private readonly IMemoryCache _memoryCache;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="GremlinDataProviderService"/> class.
        /// </summary>
        /// <param name="authenticationService">The authentication service.</param>
        /// <param name="metadataService">The metadata service.</param>
        /// <param name="loggerFactory">The logger factory.</param>
        /// <param name="memoryCache">The memory cache.</param>
        public GremlinDataProviderService(IAuthenticationService authenticationService, IServiceRegistry metadataService, ILoggerFactory loggerFactory, IMemoryCache memoryCache)
        {
            _authenticationService = authenticationService ?? throw new ArgumentNullException(nameof(authenticationService));
            _metadataService = metadataService ?? throw new ArgumentNullException(nameof(metadataService));
            _logger = loggerFactory?.CreateLogger<GremlinDataProviderService>() ?? throw new ArgumentNullException(nameof(loggerFactory));
            _memoryCache = memoryCache ?? throw new ArgumentNullException(nameof(memoryCache));
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Gets the <see cref="GremlinClient"/> for the specified service reference context.
        /// </summary>
        /// <param name="context">The service reference context.</param>
        /// <returns>The <see cref="GremlinClient"/>.</returns>
        /// <exception cref="Exception">Thrown when the context includes multiple connectors or no connectors are found.</exception>
        public GremlinClient GetDataClient(ServiceReference context)
        {
            _logger.LogTrace($"Method GetDataClient(ServiceReference.Name='{context.Name}').");
            var connectors = _metadataService.GetMetadata<GremlinStorageConnector>(context);

            // the ServiceReference context provided should only derive to a single storage connector... throw an
            //  exception if the context was too broad and includes multiple connectors; multiple connectors
            //  should be handled at a higher level...

            if (connectors is null || connectors.Count() != 1)
                throw new Exception($"Invalid connector count '{connectors?.Count()}' discovered with ServiceReference context '{context.Name}' of type '{context.GetType().Name}'.");

            var connector = connectors.Single();

            GremlinClient? client;

            if (_memoryCache.TryGetValue(connector, out client) && client is not null)
            {
                _logger.LogTrace("Service connector cache hit; leveraging cached connection.");
                return client;
            }

            _logger.LogTrace("Initializing service connector...");
            string containerLink = "/dbs/" + connector.DatabaseName + "/colls/" + connector.GraphName;

            var gremlinServer = new GremlinServer(connector.DatabaseUri, 443, true, containerLink);
            gremlinServer = _authenticationService.AuthenticateClient(context, gremlinServer);

            var connectionPoolSettings = new ConnectionPoolSettings()
            {
                MaxInProcessPerConnection = 30,
                PoolSize = 30,
                ReconnectionAttempts = 3,
                ReconnectionBaseDelay = TimeSpan.FromMilliseconds(100)
            };

            var webSocketConfiguration =
                new Action<ClientWebSocketOptions>(options =>
                {
                    options.KeepAliveInterval = TimeSpan.FromSeconds(10);
                });

            IMessageSerializer serializer = new GremlinMessageSerializer();

            client = new GremlinClient(
                gremlinServer,
                serializer,
                connectionPoolSettings,
                webSocketConfiguration);

            _memoryCache.Set(connector, client);

            return client;
        }

        #endregion

    }

}