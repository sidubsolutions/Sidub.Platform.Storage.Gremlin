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
using Sidub.Platform.Storage.Queries;
using Sidub.Platform.Storage.Services;
using System.Text.Json;

#endregion

namespace Sidub.Platform.Storage.Handlers.Gremlin
{
    /// <summary>
    /// Handles the execution of queries for retrieving entities from a Gremlin storage.
    /// </summary>
    /// <typeparam name="TEntity">The type of entity being queried.</typeparam>
    public class EntityQueryHandler<TEntity> : IRecordQueryHandler<IRecordQuery<TEntity>, TEntity> where TEntity : IEntity
    {

        #region Member variables

        private readonly IServiceRegistry _metadataService;
        private readonly IDataProviderService<GremlinClient> _dataProviderService;
        private readonly IEntitySerializerService _entitySerializerService;
        private readonly IFilterService<GremlinFilterConfiguration> _filterService;
        private readonly IEntityPartitionService _entityPartitionService;

        private StorageServiceReference? _ServiceReference;
        private IRecordQuery<TEntity>? _query;

        #endregion

        #region Public properties

        /// <summary>
        /// Gets or sets the storage service reference.
        /// </summary>
        public StorageServiceReference ServiceReference { set => _ServiceReference = value; }

        /// <summary>
        /// Gets or sets the query to be executed.
        /// </summary>
        public IRecordQuery<TEntity> Query { set => _query = value; }

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="EntityQueryHandler{TEntity}"/> class.
        /// </summary>
        /// <param name="dataProviderService">The data provider service.</param>
        /// <param name="metadataService">The metadata service.</param>
        /// <param name="entitySerializerService">The entity serializer service.</param>
        /// <param name="filterService">The filter service.</param>
        /// <param name="entityPartitionService">The entity partition service.</param>
        public EntityQueryHandler(IDataProviderService<GremlinClient> dataProviderService, IServiceRegistry metadataService, IEntitySerializerService entitySerializerService, IFilterService<GremlinFilterConfiguration> filterService, IEntityPartitionService entityPartitionService)
        {
            _dataProviderService = dataProviderService;
            _metadataService = metadataService;
            _entitySerializerService = entitySerializerService;
            _filterService = filterService;
            _entityPartitionService = entityPartitionService;

            _ServiceReference = null;
            _query = null;
        }

        #endregion

        #region Public async methods

        /// <summary>
        /// Executes the query and retrieves a single entity.
        /// </summary>
        /// <param name="queryService">The query service.</param>
        /// <returns>The retrieved entity.</returns>
        public async Task<TEntity?> Get(IQueryService queryService)
        {
            if (_ServiceReference is null)
                throw new Exception("Undefined storage ServiceReference on query handler.");

            if (_query is null)
                throw new Exception("Undefined query on query handler.");

            var entityLabel = EntityTypeHelper.GetEntityName<TEntity>();
            var fields = EntityTypeHelper.GetEntityFields<TEntity>();
            var relations = EntityTypeHelper.GetEntityRelations<TEntity>();
            var recordRelations = relations.Where(x => !x.IsEnumerableRelation);
            var enumerableRelations = relations.Where(x => x.IsEnumerableRelation);

            var filter = _query.GetFilter();
            var filterString = _filterService.GetFilterString(filter);

            GremlinStorageConnector storageConnector = _metadataService.GetMetadata<GremlinStorageConnector>(_ServiceReference).SingleOrDefault()
                ?? throw new Exception("Storage connector not initialized.");

            if (storageConnector.SerializationLanguage != SerializationLanguageType.Json)
                throw new NotImplementedException("JSON serialization is only supported on Gremlin handlers.");

            string? partitionValue;

            // if the entity is partitioned, check the query to see if it is a partitioned query... if the query is not
            //  partitioned, assume we are retrieving all partitions (achieved by leaving partitionValue null, see later)...
            if (EntityTypeHelper.IsEntityPartitioned<TEntity>())
            {
                if (_query is IQueryPartition)
                {
                    // the entity is partitioned and the query defines a partition value...
                    IQueryPartition queryPartition = (_query as IQueryPartition)!; // null hint...
                    partitionValue = queryPartition.GetQueryPartitionValue();
                }
                else
                {
                    // the entity is partitioned, but the query does not define a partition...
                    partitionValue = null;
                }
            }
            else
            {
                // if the data entity is not partitioned, apply a blank partition key...
                partitionValue = "";
            }

            var serializerOptions = SerializerOptions.Default<JsonEntitySerializerOptions>();
            var serializer = _entitySerializerService.GetEntitySerializer<TEntity>(serializerOptions) as IEntityJsonSerializer ?? throw new Exception("Could not get json serializer.");
            JsonSerializerOptions jsonSerializerOptions = serializer.GetJsonSerializerOptions(serializerOptions);

            string relationFilter = string.Empty;

            string commandString = $"g.V()";
            commandString += partitionValue is null
                ? $".hasLabel('{entityLabel}')"
                : $".has('{entityLabel}', '{storageConnector.PartitionKeyFieldName}', '{partitionValue}')";

            if (!string.IsNullOrEmpty(filterString))
                commandString += $".where({filterString})";

            commandString += relationFilter;

            byte[]? responseData;
            string typeDelimiterSelect = string.Empty;
            string typeDelimiter = string.Empty;

            // if we're dealing with an abstract entity, we need to retrieve type information from the storage layer
            //  regarding all the abstact types we have... then we'll execute the actual query for retrieving data...
            if (EntityTypeHelper.IsEntityAbstract<TEntity>())
            {
                var typeCommandString = commandString;

                typeCommandString += $".project('{TypeDiscriminatorEntityField.Instance.FieldName}')";
                typeCommandString += $".by(values('{TypeDiscriminatorEntityField.Instance.FieldName}'))";

                string? distinctType = null;

                var client2 = _dataProviderService.GetDataClient(_ServiceReference);
                //using (var client = _dataProviderService.GetDataClient(_ServiceReference))
                //{
                var response2 = await client2.SubmitAsync<Dictionary<string, object>>(typeCommandString);

                if (response2.Count != 1)
                {
                    throw new Exception($"Invalid result count of '{response2.Count}' returned by identity query '{filterString}'.");
                }

                var responseRecord2 = response2.SelectMany(x => x.Values.Select(y => y.ToString() ?? string.Empty));
                distinctType = responseRecord2.Single();
                //}

                // TODO - here for effect, only used in enumerable query...
                //typeDelimiter = $".has('{TypeDiscriminatorEntityField.Instance.FieldName}', '{distinctType}')";

                var typeDiscriminator = TypeDiscriminator.FromString(distinctType);

                //var TTypeDelimiter = Type.GetType(distinctType);

                fields = EntityTypeHelper.GetEntityFields(typeDiscriminator).Concat(new[] { TypeDiscriminatorEntityField.Instance });
            }

            commandString += $".project('{string.Join("','", fields.Select(x => x.FieldName).Concat(recordRelations.Select(x => x.Name)).Concat(enumerableRelations.Select(x => x.Name)))}')";
            commandString += string.Join(string.Empty, fields.Select(x => $".by(values('{x.FieldName}').coalesce(unfold(), constant('!dbNull')).fold())"));
            commandString += string.Join(string.Empty, recordRelations.Select(GremlinQueryHelper.BuildRelationQuery));
            commandString += string.Join(string.Empty, enumerableRelations.Select(GremlinQueryHelper.BuildRelationQuery));
            commandString += ".unfold()";
            //commandString += ".local(where(select(values).unfold().coalesce(constant('!notNull'), constant('!dbNull')).is(without('!dbNull')))";
            commandString += ".group().by(select(keys)).by(select(values).unfold())";
            //commandString += ")";

            var client = _dataProviderService.GetDataClient(_ServiceReference);
            //using (var client = _dataProviderService.GetDataClient(_ServiceReference))
            //{
            var response = await client.SubmitAsync<Dictionary<string, object>>(commandString);

            if (response.Count > 1)
            {
                throw new Exception($"Invalid result count of '{response.Count}' returned by identity query '{filterString}'.");
            }

            var responseRecord = response.SingleOrDefault();

            responseData = responseRecord is null || responseRecord.Count == 0 ? null : JsonSerializer.SerializeToUtf8Bytes(responseRecord, jsonSerializerOptions);
            //}

            TEntity? entity = default(TEntity);

            if (responseData is null)
                return default(TEntity);

            entity = _entitySerializerService.Deserialize<TEntity>(responseData, serializerOptions).With(x => x.IsRetrievedFromStorage = true);

            entity = await QueryHandlerHelper.AssignEntityReferenceProviders(queryService, _ServiceReference, entity);



            return entity;
        }

        async IAsyncEnumerable<TEntity> IQueryHandler<IRecordQuery<TEntity>, TEntity>.Get(IQueryService queryService, QueryParameters? queryParameters)
        {
            var result = await Get(queryService);

            if (result is not null)
                yield return result;

            yield break;
        }

        #endregion

    }

}
