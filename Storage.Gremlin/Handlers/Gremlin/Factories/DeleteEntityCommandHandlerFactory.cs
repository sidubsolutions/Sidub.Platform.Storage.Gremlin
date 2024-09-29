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

using Sidub.Platform.Core.Services;
using Sidub.Platform.Filter.Parsers.Gremlin;
using Sidub.Platform.Filter.Services;
using Sidub.Platform.Storage.Commands;
using Sidub.Platform.Storage.Commands.Responses;
using Sidub.Platform.Storage.Services.Gremlin;

#endregion

namespace Sidub.Platform.Storage.Handlers.Gremlin.Factories
{

    /// <summary>
    /// Factory class for creating instances of <see cref="DeleteEntityCommandHandler{T}"/>.
    /// </summary>
    public class DeleteEntityCommandHandlerFactory : ICommandHandlerFactory<GremlinStorageConnector>
    {

        #region Member variables

        private readonly IServiceRegistry _metadataService;
        private readonly GremlinDataProviderService _dataProviderService;
        private readonly IEntitySerializerService _entitySerializerService;
        private readonly IFilterService<GremlinFilterConfiguration> _filterService;
        private readonly IEntityPartitionService _entityPartitionService;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteEntityCommandHandlerFactory"/> class.
        /// </summary>
        /// <param name="metadataService">The metadata service.</param>
        /// <param name="dataProviderService">The data provider service.</param>
        /// <param name="entitySerializerService">The entity serializer service.</param>
        /// <param name="filterService">The filter service.</param>
        /// <param name="entityPartitionService">The entity partition service.</param>
        public DeleteEntityCommandHandlerFactory(IServiceRegistry metadataService, GremlinDataProviderService dataProviderService, IEntitySerializerService entitySerializerService, IFilterService<GremlinFilterConfiguration> filterService, IEntityPartitionService entityPartitionService)
        {
            _metadataService = metadataService;
            _dataProviderService = dataProviderService;
            _entitySerializerService = entitySerializerService;
            _filterService = filterService;
            _entityPartitionService = entityPartitionService;
        }

        #endregion

        #region Public methods

        /// <summary>
        /// Determines whether the specified command type is handled by this factory.
        /// </summary>
        /// <typeparam name="TCommand">The type of the command.</typeparam>
        /// <typeparam name="TResult">The type of the command result.</typeparam>
        /// <returns><c>true</c> if the specified command type is handled; otherwise, <c>false</c>.</returns>
        public bool IsHandled<TCommand, TResult>()
            where TCommand : ICommand<TResult>
            where TResult : ICommandResponse
        {
            var T = typeof(TCommand);

            if (typeof(DeleteEntityCommand<>) == T.GetGenericTypeDefinition())
                return true;

            return false;
        }

        /// <summary>
        /// Creates an instance of the command handler for the specified command type.
        /// </summary>
        /// <typeparam name="TCommand">The type of the command.</typeparam>
        /// <typeparam name="TResult">The type of the command result.</typeparam>
        /// <returns>An instance of the command handler for the specified command type.</returns>
        /// <exception cref="Exception">Thrown when the specified command type is not handled by this factory.</exception>
        public ICommandHandler<TCommand, TResult> Create<TCommand, TResult>()
            where TCommand : ICommand<TResult>
            where TResult : ICommandResponse
        {
            if (!IsHandled<TCommand, TResult>())
                throw new Exception("Unhandled type.");

            var genericArgs = typeof(TCommand).GenericTypeArguments;
            var parentType = genericArgs[0];
            //(IDataProviderService<GremlinClient> dataProviderService, IServiceRegistry metadataService, IEntityPartitionService entityPartitionService, IEntitySerializerService entitySerializerService, IFilterService<GremlinFilterConfiguration> filterService)
            var handlerType = typeof(DeleteEntityCommandHandler<>).MakeGenericType(new[] { parentType });
            var handlerParameters = new object[] { _dataProviderService, _metadataService, _entityPartitionService, _entitySerializerService, _filterService };
            var handler = Activator.CreateInstance(handlerType, handlerParameters);

            if (handler is not ICommandHandler<TCommand, TResult> handlerCast)
                throw new Exception("Command handler cannot cast to ICommandHandler<TCommand, TResult>; check factory validation and handler instantiation.");

            return handlerCast;
        }

        #endregion
    }

}
