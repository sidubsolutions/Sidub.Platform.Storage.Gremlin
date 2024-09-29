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
    /// Handles the command to save an entity in Gremlin storage.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity.</typeparam>
    public class EntitySaveCommandHandler<TEntity> : ICommandHandler<SaveEntityCommand<TEntity>, SaveEntityCommandResponse<TEntity>> where TEntity : IEntity
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
        /// Initializes a new instance of the <see cref="EntitySaveCommandHandler{TEntity}"/> class.
        /// </summary>
        /// <param name="dataProviderService">The data provider service.</param>
        /// <param name="metadataService">The metadata service.</param>
        /// <param name="entityPartitionService">The entity partition service.</param>
        /// <param name="entitySerializerService">The entity serializer service.</param>
        /// <param name="filterService">The filter service.</param>
        public EntitySaveCommandHandler(IDataProviderService<GremlinClient> dataProviderService, IServiceRegistry metadataService, IEntityPartitionService entityPartitionService, IEntitySerializerService entitySerializerService, IFilterService<GremlinFilterConfiguration> filterService)
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
        /// Executes the save entity command.
        /// </summary>
        /// <param name="ServiceReference">The storage service reference.</param>
        /// <param name="command">The save entity command.</param>
        /// <param name="queryService">The query service.</param>
        /// <returns>The response of the save entity command.</returns>
        public async Task<SaveEntityCommandResponse<TEntity>> Execute(StorageServiceReference ServiceReference, SaveEntityCommand<TEntity> command, IQueryService queryService)
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

            // build the data command...
            var cmdUpsert = $"g.V()" + rootEntityFilter;
            cmdUpsert += ".fold()";
            cmdUpsert += $".coalesce(unfold(), addV('{entityLabel}')";
            cmdUpsert += $".property('{storageConnector.PartitionKeyFieldName}', '{partitionValue}')";
            cmdUpsert += string.Concat(keyDictionary.Select(x => $".property('{x.Key}', {GremlinQueryHelper.WrapGremlinValue(x.Value)})"));
            cmdUpsert += ")";
            cmdUpsert += string.Concat(payloadDictionary.Select(x => $".property('{x.Key}', {GremlinQueryHelper.WrapGremlinValue(x.Value)})"));
            cmdUpsert += ".unfold()";
            cmdUpsert += $".project('{string.Join("','", entityFields.Select(x => x.FieldName).Concat(recordRelations.Select(x => x.Name)).Concat(enumerableRelations.Select(x => x.Name)))}')";
            cmdUpsert += string.Concat(entityFields.Select(x => $".by(values('{x.FieldName}').fold().coalesce(unfold(), constant('!dbNull')).fold())"));
            cmdUpsert += string.Join(string.Empty, recordRelations.Select(GremlinQueryHelper.BuildRelationQuery));
            cmdUpsert += string.Join(string.Empty, enumerableRelations.Select(GremlinQueryHelper.BuildRelationQuery));
            cmdUpsert += ".unfold()";
            //commandString += ".local(where(select(values).unfold().coalesce(constant('!notNull'), constant('!dbNull')).is(without('!dbNull')))";
            cmdUpsert += ".group().by(select(keys)).by(select(values).unfold())";
            //commandString += ")";

            // open a data client for the given ServiceReference...
            IDictionary<string, object?> responseData;

            var client = _dataProviderService.GetDataClient(ServiceReference);
            var response = await client.SubmitAsync<Dictionary<string, object>>(cmdUpsert);

            if (response.Count != 1)
                throw new Exception($"Invalid result returned from save command; entity result cannot be parsed.");

            responseData = response.Single() as IDictionary<string, object?>;

            serializerOptions = serializerOptions.With(x => x.FieldSerialization = EntityFieldType.All);
            var entityResult = _entitySerializerService.DeserializeDictionary<TEntity>(responseData, serializerOptions).With(x => x.IsRetrievedFromStorage = true);

            foreach (var relation in recordRelations)
            {
                // one-to-one, so only one reference will be returned..
                var entityRef = EntityTypeHelper.GetEntityRelationRecord(command.Entity, relation)
                    ?? throw new Exception("Failed to get entity relation record reference; if value is intended to be null, it should be assigned IEntityReference.Null value.");

                var saveRelation = SaveEntityRelationCommand.Create(relation, command.Entity, entityRef);
                var saveRelationResult = await queryService.Execute(ServiceReference, saveRelation);

                if (!saveRelationResult.IsSuccessful)
                    throw new Exception($"Error saving entity relation '{relation.Name}'.");

            }

            foreach (var relation in enumerableRelations)
            {
                // one-to-many / many-to-many
                var entityRefs = EntityTypeHelper.GetEntityRelationEnumerable(command.Entity, relation)
                    ?? throw new Exception("Failed to get entity relation list reference; if value is intended to be null, it should be assigned an empty IEntityReferenceList value.");

                foreach (var entityRef in entityRefs)
                {
                    var saveRelation = SaveEntityRelationCommand.Create(relation, command.Entity, entityRef);
                    var saveRelationResult = await queryService.Execute(ServiceReference, saveRelation);

                    if (!saveRelationResult.IsSuccessful)
                        throw new Exception($"Error saving entity relation '{relation.Name}'.");
                }

                foreach (var removedEntityRef in entityRefs.RemovedReferences)
                {
                    var saveRelation = SaveEntityRelationCommand.Create(relation, command.Entity, removedEntityRef);
                    var saveRelationResult = await queryService.Execute(ServiceReference, saveRelation);

                    if (!saveRelationResult.IsSuccessful)
                        throw new Exception($"Error saving entity relation '{relation.Name}'.");
                }

                entityRefs.Commit();
            }

            // note, if there are relations, we need to re-execute the upsert (net no data impact) to re-retrieve the record
            //  with relationship information intact... if we don't do this, we won't have relationship information since the record upsert occurred before
            //  the relationships; we also must do an insert before relationships because we cannot create a relationship without the record being present...
            if (relations.Any())
            {
                response = await client.SubmitAsync<Dictionary<string, object>>(cmdUpsert);

                if (response.Count != 1)
                    throw new Exception($"Invalid result returned from save command; entity result cannot be parsed.");

                responseData = response.Single() as IDictionary<string, object?>;

                entityResult = _entitySerializerService.DeserializeDictionary<TEntity>(responseData, serializerOptions).With(x => x.IsRetrievedFromStorage = true);
            }

            entityResult = await QueryHandlerHelper.AssignEntityReferenceProviders(queryService, ServiceReference, entityResult);

            var saveResponse = new SaveEntityCommandResponse<TEntity>(true, entityResult);

            return saveResponse;

        }

        #endregion

    }

}
