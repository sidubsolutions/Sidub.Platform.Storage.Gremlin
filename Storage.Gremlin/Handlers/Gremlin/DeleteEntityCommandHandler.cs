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
using Sidub.Platform.Core.Entity.Relations;
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
    /// Represents a command handler for deleting an entity in Gremlin storage.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    public class DeleteEntityCommandHandler<TEntity> : ICommandHandler<DeleteEntityCommand<TEntity>, DeleteEntityCommandResponse> where TEntity : IEntity
    {

        #region Member variables

        private readonly IDataProviderService<GremlinClient> _dataProviderService;
        private readonly IServiceRegistry _metadataService;
        private readonly IEntityPartitionService _entityPartitionService;
        private readonly IEntitySerializerService _entitySerializerService;
        private readonly IFilterService<GremlinFilterConfiguration> _filterService;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DeleteEntityCommandHandler{TEntity}"/> class.
        /// </summary>
        /// <param name="dataProviderService">The data provider service.</param>
        /// <param name="metadataService">The metadata service.</param>
        /// <param name="entityPartitionService">The entity partition service.</param>
        /// <param name="entitySerializerService">The entity serializer service.</param>
        /// <param name="filterService">The filter service.</param>
        public DeleteEntityCommandHandler(IDataProviderService<GremlinClient> dataProviderService, IServiceRegistry metadataService, IEntityPartitionService entityPartitionService, IEntitySerializerService entitySerializerService, IFilterService<GremlinFilterConfiguration> filterService)
        {
            _dataProviderService = dataProviderService;
            _metadataService = metadataService;
            _entityPartitionService = entityPartitionService;
            _entitySerializerService = entitySerializerService;
            _filterService = filterService;
        }

        #endregion

        #region Public async methods

        /// <summary>
        /// Executes the delete entity command.
        /// </summary>
        /// <param name="ServiceReference">The storage service reference.</param>
        /// <param name="command">The delete entity command.</param>
        /// <param name="queryService">The query service.</param>
        /// <returns>The delete entity command response.</returns>
        public async Task<DeleteEntityCommandResponse> Execute(StorageServiceReference ServiceReference, DeleteEntityCommand<TEntity> command, IQueryService queryService)
        {
            // retrieve storage metadata - we're dealing with a storage ServiceReference context so we should only have one metadata below...
            GremlinStorageConnector storageConnector = _metadataService.GetMetadata<GremlinStorageConnector>(ServiceReference).SingleOrDefault()
                ?? throw new Exception("Storage connector not initialized.");

            if (storageConnector.SerializationLanguage != SerializationLanguageType.Json)
                throw new NotImplementedException("JSON serialization is only supported on Gremlin handlers.");

            if (string.IsNullOrEmpty(storageConnector.PartitionKeyFieldName))
                throw new Exception("Partition identifier must be populated on Gremlin storage connector.");

            var entityLabel = EntityTypeHelper.GetEntityName(command.Entity);
            var entityFields = EntityTypeHelper.GetEntityFields(command.Entity).ToList();

            if (EntityTypeHelper.IsEntityAbstract(command.Entity) || EntityTypeHelper.IsEntityAbstract<TEntity>())
            {
                entityFields.Add(TypeDiscriminatorEntityField.Instance);
            }

            // if an entity does not implement a partition strategy, store under an empty partition key... we shouldn't prevent entities from
            //  saving to gremlin simply because a partition strategy is not employed...
            var partitionValue = _entityPartitionService.GetPartitionValue(command.Entity) ?? "";

            if (partitionValue is null)
                partitionValue = "";

            var serializerOptions = SerializerOptions.New<JsonEntitySerializerOptions>();

            // retrieve dictionary of entity key values...
            serializerOptions = serializerOptions.With(x => { x.FieldSerialization = EntityFieldType.Keys; x.IncludeTypeInfo = false; });
            var keyDictionary = _entitySerializerService.SerializeDictionary(command.Entity, serializerOptions);

            // ensure there are no null key values...
            if (keyDictionary.Any(x => x.Value is null))
                throw new Exception("A null entity key value was encountered.");

            // retrieve dictionary of entity field values...
            serializerOptions = serializerOptions.With(x => { x.FieldSerialization = EntityFieldType.Fields; x.IncludeTypeInfo = true; });
            var payloadDictionary = _entitySerializerService.SerializeDictionary(command.Entity, serializerOptions);

            // build a command string to save base entity...
            var rootEntityFilter = $".has('{entityLabel}', '{storageConnector.PartitionKeyFieldName}', '{partitionValue}')";
            rootEntityFilter += string.Concat(keyDictionary.Select(x => $".has('{x.Key}', {GremlinQueryHelper.WrapGremlinValue(x.Value)})"));

            var relations = EntityTypeHelper.GetEntityRelations<TEntity>();
            var recordRelations = relations.Where(x => !x.IsEnumerableRelation);
            var enumerableRelations = relations.Where(x => x.IsEnumerableRelation);

            foreach (var relation in recordRelations)
            {

                var val = EntityTypeHelper.GetEntityRelationRecord(command.Entity, relation)
                    ?? throw new Exception($"Failed retrieving relation '{relation.Name}'");

                // if the relation is marked as deleted, we can skip validation...
                if (val.Action == EntityRelationActionType.Delete)
                    continue;

                if (val.HasValue())
                    throw new Exception($"Cannot delete entity '{entityLabel}' because it has a relationship to '{relation.Name}'.");
            }

            foreach (var relation in enumerableRelations)
            {
                var val = EntityTypeHelper.GetEntityRelationEnumerable(command.Entity, relation)
                    ?? throw new Exception($"Failed retrieving relation '{relation.Name}'");

                if (val.Any(x => x.Action != EntityRelationActionType.Delete))
                    throw new Exception($"Cannot delete entity '{entityLabel}' because it has a relationship to '{relation.Name}'.");
            }



            // build the data command...
            var cmdUpsert = $"g.V()" + rootEntityFilter + ".drop()";

            // open a data client for the given ServiceReference...
            IDictionary<string, object?> responseData;

            var client = _dataProviderService.GetDataClient(ServiceReference);
            var response = await client.SubmitAsync<Dictionary<string, object>>(cmdUpsert);


            // TODO - recursive delete in some situations?
            //foreach (var relation in recordRelations)
            //{
            //    // one-to-one, so only one reference will be returned..
            //    var entityRef = EntityTypeHelper.GetEntityRelationRecord(command.Entity, relation)
            //        ?? throw new Exception("Failed to get entity relation record reference; if value is intended to be null, it should be assigned IEntityReference.Null value.");

            //    var saveRelation = ISaveEntityRelationCommand.Create(relation, command.Entity, entityRef);
            //    var saveRelationResult = await queryService.Execute(ServiceReference, saveRelation);

            //    if (!saveRelationResult.IsSuccessful)
            //        throw new Exception($"Error saving entity relation '{relation.Name}'.");

            //}

            //foreach (var relation in enumerableRelations)
            //{
            //    // one-to-many / many-to-many
            //    var entityRefs = EntityTypeHelper.GetEntityRelationEnumerable(command.Entity, relation)
            //        ?? throw new Exception("Failed to get entity relation list reference; if value is intended to be null, it should be assigned an empty IEntityReferenceList value.");

            //    foreach (var entityRef in entityRefs)
            //    {
            //        var saveRelation = ISaveEntityRelationCommand.Create(relation, command.Entity, entityRef);
            //        var saveRelationResult = await queryService.Execute(ServiceReference, saveRelation);

            //        if (!saveRelationResult.IsSuccessful)
            //            throw new Exception($"Error saving entity relation '{relation.Name}'.");
            //    }

            //    foreach (var removedEntityRef in entityRefs.RemovedReferences)
            //    {
            //        var saveRelation = ISaveEntityRelationCommand.Create(relation, command.Entity, removedEntityRef);
            //        var saveRelationResult = await queryService.Execute(ServiceReference, saveRelation);

            //        if (!saveRelationResult.IsSuccessful)
            //            throw new Exception($"Error saving entity relation '{relation.Name}'.");
            //    }

            //    entityRefs.ClearRemovedReferences();
            //}

            // note, if there are relations, we need to re-execute the upsert (net no data impact) to re-retrieve the record
            //  with relationship information intact... if we don't do this, we won't have relationship information since the record upsert occurred before
            //  the relationships; we also must do an insert before relationships because we cannot create a relationship without the record being present...
            //if (relations.Any())
            //{
            //    response = await client.SubmitAsync<Dictionary<string, object>>(cmdUpsert);

            //    if (response.Count != 1)
            //        throw new Exception($"Invalid result returned from save command; entity result cannot be parsed.");

            //    responseData = response.Single() as IDictionary<string, object?>;

            //    entityResult = _entitySerializerService.DeserializeDictionary<TEntity>(responseData, serializerOptions).With(x => x.IsRetrievedFromStorage = true);
            //}

            //entityResult = await QueryHandlerReferenceHelper.AssignEntityReferenceProviders(queryService, ServiceReference, entityResult);

            var isSuccessful = response.StatusAttributes["x-ms-status-code"].ToString() == "200";
            var saveResponse = new DeleteEntityCommandResponse(isSuccessful);

            return saveResponse;

        }

        #endregion

    }

}
