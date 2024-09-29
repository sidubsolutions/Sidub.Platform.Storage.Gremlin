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

using Sidub.Platform.Core.Entity;
using Sidub.Platform.Core.Services;
using Sidub.Platform.Filter.Parsers.Gremlin;
using Sidub.Platform.Filter.Services;
using Sidub.Platform.Storage.Commands;
using Sidub.Platform.Storage.Commands.Responses;
using Sidub.Platform.Storage.Connectors;
using Sidub.Platform.Storage.Handlers;
using Sidub.Platform.Storage.Handlers.Gremlin;
using Sidub.Platform.Storage.Queries;

#endregion

namespace Sidub.Platform.Storage.Services.Gremlin
{

    /// <summary>
    /// Represents a service for handling data operations in Gremlin storage.
    /// </summary>
    public class GremlinDataHandlerService : IDataHandlerService
    {

        #region Member variables

        private readonly IFilterService<GremlinFilterConfiguration> _filterService;
        private readonly GremlinDataProviderService _dataProviderService;
        private readonly IServiceRegistry _metadataService;
        private readonly IEntityPartitionService _entityPartitionService;
        private readonly IEntitySerializerService _entitySerializerService;
        private readonly IReadOnlyList<ICommandHandlerFactory<GremlinStorageConnector>> _commandHandlers;
        private readonly IReadOnlyList<IQueryHandlerFactory<GremlinStorageConnector>> _queryHandlers;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="GremlinDataHandlerService"/> class.
        /// </summary>
        /// <param name="filterService">The filter service.</param>
        /// <param name="gremlinService">The Gremlin data provider service.</param>
        /// <param name="entityMetadataService">The entity metadata service.</param>
        /// <param name="entityPartitionService">The entity partition service.</param>
        /// <param name="entitySerializerService">The entity serializer service.</param>
        /// <param name="commandHandlers">The command handlers.</param>
        /// <param name="queryHandlers">The query handlers.</param>
        public GremlinDataHandlerService(
            IFilterService<GremlinFilterConfiguration> filterService,
            GremlinDataProviderService gremlinService,
            IServiceRegistry entityMetadataService,
            IEntityPartitionService entityPartitionService,
            IEntitySerializerService entitySerializerService,
            IEnumerable<ICommandHandlerFactory<GremlinStorageConnector>> commandHandlers,
            IEnumerable<IQueryHandlerFactory<GremlinStorageConnector>> queryHandlers)
        {
            _filterService = filterService;
            _dataProviderService = gremlinService;
            _metadataService = entityMetadataService;
            _entityPartitionService = entityPartitionService;
            _entitySerializerService = entitySerializerService;
            _commandHandlers = commandHandlers.ToList().AsReadOnly();
            _queryHandlers = queryHandlers.ToList().AsReadOnly();
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Gets the save command handler for the specified entity type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="queryService">The query service.</param>
        /// <returns>The save command handler.</returns>
        public ICommandHandler<SaveEntityCommand<TEntity>, SaveEntityCommandResponse<TEntity>> GetSaveCommandHandler<TEntity>(IQueryService queryService) where TEntity : IEntity
        {
            var handler = new EntitySaveCommandHandler<TEntity>(_dataProviderService, _metadataService, _entityPartitionService, _entitySerializerService, _filterService);

            return handler;
        }

        /// <summary>
        /// Gets the entity query handler for the specified entity type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="queryService">The query service.</param>
        /// <returns>The entity query handler.</returns>
        public IRecordQueryHandler<IRecordQuery<TEntity>, TEntity> GetEntityQueryHandler<TEntity>(IQueryService queryService) where TEntity : IEntity
        {
            var handler = new EntityQueryHandler<TEntity>(_dataProviderService, _metadataService, _entitySerializerService, _filterService, _entityPartitionService);

            return handler;
        }

        /// <summary>
        /// Gets the entities query handler for the specified entity type.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="queryService">The query service.</param>
        /// <returns>The entities query handler.</returns>
        public IEnumerableQueryHandler<IEnumerableQuery<TEntity>, TEntity> GetEntitiesQueryHandler<TEntity>(IQueryService queryService) where TEntity : class, IEntity
        {
            var handler = new EntitiesQueryHandler<TEntity>(_dataProviderService, _metadataService, _entitySerializerService, _filterService, _entityPartitionService);

            return handler;
        }

        /// <summary>
        /// Gets the query handler for the specified query type and response type.
        /// </summary>
        /// <typeparam name="TQuery">The type of the query.</typeparam>
        /// <typeparam name="TResponse">The type of the response.</typeparam>
        /// <param name="queryService">The query service.</param>
        /// <returns>The query handler.</returns>
        public IQueryHandler<TQuery, TResponse> GetQueryHandler<TQuery, TResponse>(IQueryService queryService)
            where TQuery : IQuery<TResponse>
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the command handler for the specified command type and response type.
        /// </summary>
        /// <typeparam name="TCommand">The type of the command.</typeparam>
        /// <typeparam name="TResponse">The type of the response.</typeparam>
        /// <param name="queryService">The query service.</param>
        /// <returns>The command handler.</returns>
        public ICommandHandler<TCommand, TResponse>? GetCommandHandler<TCommand, TResponse>(IQueryService queryService)
            where TCommand : ICommand<TResponse>
            where TResponse : ICommandResponse
        {
            ICommandHandler<TCommand, TResponse>? result;

            var factory = _commandHandlers.SingleOrDefault(x => x.IsHandled<TCommand, TResponse>());
            result = factory?.Create<TCommand, TResponse>();

            return result;
        }

        /// <summary>
        /// Determines whether the specified connector is handled by this service.
        /// </summary>
        /// <param name="connector">The storage connector.</param>
        /// <returns>True if the connector is handled, otherwise false.</returns>
        bool IDataHandlerService.IsHandled(IStorageConnector connector)
        {
            return connector is GremlinStorageConnector;
        }

        #endregion

    }

}
