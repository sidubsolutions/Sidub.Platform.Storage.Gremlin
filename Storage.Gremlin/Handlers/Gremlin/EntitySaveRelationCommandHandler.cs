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
using Sidub.Platform.Core;
using Sidub.Platform.Core.Entity;
using Sidub.Platform.Core.Extensions;
using Sidub.Platform.Core.Serializers;
using Sidub.Platform.Core.Serializers.Json;
using Sidub.Platform.Core.Services;
using Sidub.Platform.Filter.Parsers.Gremlin;
using Sidub.Platform.Filter.Services;
using Sidub.Platform.Storage.Commands;
using Sidub.Platform.Storage.Commands.Responses;
using Sidub.Platform.Storage.Services;

#endregion

namespace Sidub.Platform.Storage.Handlers.Gremlin
{

    /// <summary>
    /// Handles the command to save a relation between two entities in Gremlin storage.
    /// </summary>
    /// <typeparam name="TParent">The type of the parent entity.</typeparam>
    /// <typeparam name="TRelated">The type of the related entity.</typeparam>
    public class EntitySaveRelationCommandHandler<TParent, TRelated> : ICommandHandler<SaveEntityRelationCommand<TParent, TRelated>, SaveEntityRelationCommandResponse>
        where TParent : class, IEntity
        where TRelated : class, IEntity
    {

        #region Member variables

        private readonly IServiceRegistry _metadataService;
        private readonly IDataProviderService<GremlinClient> _dataProviderService;
        private readonly IEntityPartitionService _entityPartitionService;
        private readonly IEntitySerializerService _entitySerializerService;
        private readonly IFilterService<GremlinFilterConfiguration> _filterService;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="EntitySaveRelationCommandHandler{TParent, TRelated}"/> class.
        /// </summary>
        /// <param name="metadataService">The service registry for metadata.</param>
        /// <param name="dataProviderService">The data provider service for Gremlin client.</param>
        /// <param name="entitySerializerService">The entity serializer service.</param>
        /// <param name="filterService">The filter service for Gremlin filter configuration.</param>
        /// <param name="entityPartitionService">The entity partition service.</param>
        public EntitySaveRelationCommandHandler(IServiceRegistry metadataService, IDataProviderService<GremlinClient> dataProviderService, IEntitySerializerService entitySerializerService, IFilterService<GremlinFilterConfiguration> filterService, IEntityPartitionService entityPartitionService)
        {
            _metadataService = metadataService;
            _dataProviderService = dataProviderService;
            _entitySerializerService = entitySerializerService;
            _filterService = filterService;
            _entityPartitionService = entityPartitionService;
        }

        #endregion

        #region Public async methods

        /// <summary>
        /// Executes the command to save a relation between two entities in Gremlin storage.
        /// </summary>
        /// <param name="ServiceReference">The storage service reference.</param>
        /// <param name="command">The save entity relation command.</param>
        /// <param name="queryService">The query service.</param>
        /// <returns>The response of the save entity relation command.</returns>
        public async Task<SaveEntityRelationCommandResponse> Execute(StorageServiceReference ServiceReference, SaveEntityRelationCommand<TParent, TRelated> command, IQueryService queryService)
        {
            // retrieve storage metadata - we're dealing with a storage ServiceReference context so we should only have one metadata below...
            GremlinStorageConnector storageConnector = _metadataService.GetMetadata<GremlinStorageConnector>(ServiceReference).SingleOrDefault()
                ?? throw new Exception("Storage connector not initialized.");

            if (storageConnector.SerializationLanguage != SerializationLanguageType.Json)
                throw new NotImplementedException("JSON serialization is only supported on Gremlin handlers.");

            if (string.IsNullOrEmpty(storageConnector.PartitionKeyFieldName))
                throw new Exception("Partition identifier must be populated on Gremlin storage connector.");

            // TODO - elaborate / improve... we cannot save a relation unless the related entity is persisted...
            if (command.RelatedEntity.HasValue())
            {
                var relatedValue = await command.RelatedEntity.Get();
                if (!relatedValue.IsRetrievedFromStorage)
                    throw new Exception("Related entity must be saved to storage before a relationship can be defined.");
            }

            var parentEntityLabel = EntityTypeHelper.GetEntityName<TParent>();
            var relatedEntityLabel = EntityTypeHelper.GetEntityName<TRelated>();

            // if an entity does not implement a partition strategy, store under an empty partition key... we shouldn't prevent entities from
            //  saving to gremlin simply because a partition strategy is not employed...
            var parentPartitionValue = _entityPartitionService.GetPartitionValue(command.ParentEntity) ?? "";

            // TODO - temporary, need to evolve the partition service to handle entity references...
            var relatedPartitionValue = _entityPartitionService.GetPartitionValue(command.RelatedEntity) ?? "";

            var serializerOptions = SerializerOptions.New<JsonEntitySerializerOptions>();

            // retrieve dictionary of entity key values...
            serializerOptions = serializerOptions.With(x => { x.FieldSerialization = EntityFieldType.Keys; x.IncludeTypeInfo = false; });
            var parentKeyDictionary = _entitySerializerService.SerializeDictionary(command.ParentEntity, serializerOptions);
            var relatedKeyDictionary = command.RelatedEntity.EntityKeys.ToDictionary(x => x.Key.FieldName, y => y.Value);

            // ensure there are no null key values...
            if (parentKeyDictionary.Any(x => x.Value is null))
                throw new Exception("A null entity key value was encountered.");

            // build a command string to save base entity...
            var rootEntityFilter = $".has('{parentEntityLabel}', '{storageConnector.PartitionKeyFieldName}', '{parentPartitionValue}')";
            rootEntityFilter += string.Concat(parentKeyDictionary.Select(x => $".has('{x.Key}', {GremlinQueryHelper.WrapGremlinValue(x.Value)})"));

            var relatedEntityFilter = $".has('{relatedEntityLabel}', '{storageConnector.PartitionKeyFieldName}', '{relatedPartitionValue}')";
            relatedEntityFilter += string.Concat(relatedKeyDictionary.Select(x => $".has('{x.Key}', {GremlinQueryHelper.WrapGremlinValue(x.Value)})"));

            // build the data command...
            var cmdUpsert = $"g.V()" + rootEntityFilter + ".as('parent')";
            cmdUpsert += ".V()" + relatedEntityFilter;

            if (command.IsDeleted)
            {
                cmdUpsert += $".inE('{command.Relation.Name}').where(outV().as('parent')).drop()";
            }
            else
            {
                cmdUpsert += $".coalesce(__.inE('{command.Relation.Name}').where(outV().as('parent')),";
                cmdUpsert += $" addE('{command.Relation.Name}').from('parent'))";
            }

            // open a data client for the given ServiceReference...
            IDictionary<string, object?> responseData;

            var client = _dataProviderService.GetDataClient(ServiceReference);
            var clientResponse = await client.SubmitAsync<Dictionary<string, object>>(cmdUpsert);

            // we expect a response count of one in all cases except deletion...
            if (!command.IsDeleted && clientResponse.Count != 1)
                throw new Exception($"Invalid result returned from save command; entity result cannot be parsed.");
            else if (!command.IsDeleted)
                responseData = clientResponse.Single() as IDictionary<string, object?>;

            // TODO evolve...
            var result = new SaveEntityRelationCommandResponse()
            {
                IsSuccessful = true
            };

            return result;
        }

        #endregion

    }

}
