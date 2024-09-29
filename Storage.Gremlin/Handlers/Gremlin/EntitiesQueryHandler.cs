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
    /// Handles the query for retrieving entities from Gremlin storage.
    /// </summary>
    /// <typeparam name="TEntity">The type of entity.</typeparam>
    public class EntitiesQueryHandler<TEntity> : IEnumerableQueryHandler<IEnumerableQuery<TEntity>, TEntity> where TEntity : class, IEntity
    {

        #region Member variables

        private readonly IServiceRegistry _metadataService;
        private readonly IDataProviderService<GremlinClient> _dataProviderService;
        private readonly IEntitySerializerService _entitySerializerService;
        private readonly IFilterService<GremlinFilterConfiguration> _filterService;
        private readonly IEntityPartitionService _entityPartitionService;
        private StorageServiceReference? _ServiceReference;
        private IEnumerableQuery<TEntity>? _query;

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="EntitiesQueryHandler{TEntity}"/> class.
        /// </summary>
        /// <param name="dataProviderService">The data provider service.</param>
        /// <param name="metadataService">The metadata service.</param>
        /// <param name="entitySerializerService">The entity serializer service.</param>
        /// <param name="filterService">The filter service.</param>
        /// <param name="entityPartitionService">The entity partition service.</param>
        public EntitiesQueryHandler(IDataProviderService<GremlinClient> dataProviderService, IServiceRegistry metadataService, IEntitySerializerService entitySerializerService, IFilterService<GremlinFilterConfiguration> filterService, IEntityPartitionService entityPartitionService)
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

        #region Public properties

        /// <summary>
        /// Gets or sets the storage service reference.
        /// </summary>
        public StorageServiceReference ServiceReference { set => _ServiceReference = value; }

        /// <summary>
        /// Gets or sets the query.
        /// </summary>
        public IEnumerableQuery<TEntity> Query { set => _query = value; }

        #endregion

        #region Public async methods

        // TODO - implement queryParameters...
        /// <summary>
        /// Retrieves entities from Gremlin storage based on the specified query.
        /// </summary>
        /// <param name="queryService">The query service.</param>
        /// <param name="queryParameters">The query parameters.</param>
        /// <returns>An asynchronous enumerable of entities.</returns>
        public async IAsyncEnumerable<TEntity> Get(IQueryService queryService, QueryParameters? queryParameters = null)
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

            if (queryParameters?.Top is not null && queryParameters?.Skip is not null)
                commandString += $".order().range({queryParameters.Skip},{queryParameters.Skip + queryParameters.Top})";
            else if (queryParameters?.Skip is not null)
                commandString += $".order().skip({queryParameters.Skip})";
            else if (queryParameters?.Top is not null)
                commandString += $".order().range(0,{queryParameters.Top})";

            commandString += $".project('{string.Join("','", fields.Select(x => x.FieldName).Concat(recordRelations.Select(x => x.Name)).Concat(enumerableRelations.Select(x => x.Name)))}')";
            commandString += string.Join(string.Empty, fields.Select(x => $".by(values('{x.FieldName}').fold().coalesce(unfold(), constant('!dbNull')).fold())"));
            commandString += string.Join(string.Empty, recordRelations.Select(GremlinQueryHelper.BuildRelationQuery));
            commandString += string.Join(string.Empty, enumerableRelations.Select(GremlinQueryHelper.BuildRelationQuery));
            commandString += ".local(";
            commandString += "unfold()";
            //commandString += ".local(where(select(values).unfold().coalesce(constant('!notNull'), constant('!dbNull')).is(without('!dbNull')))";
            commandString += ".group().by(select(keys)).by(select(values).unfold())";
            //commandString += ")";
            commandString += ")";

            byte[] entitiesData;

            bool isNullList = false;
            IEnumerable<TEntity> entities;

            var client = _dataProviderService.GetDataClient(_ServiceReference);
            //using (var client = _dataProviderService.GetDataClient(_ServiceReference))
            //{
            var queryResult = await client.SubmitAsync<Dictionary<string, object>>(commandString);

            if (queryResult.Count == 1)
            {
                if (queryResult.Single().Count == 0)
                {
                    isNullList = true;
                }
            }

            entitiesData = JsonSerializer.SerializeToUtf8Bytes(queryResult.ToList(), jsonSerializerOptions);
            entities = isNullList ? Enumerable.Empty<TEntity>() : _entitySerializerService.DeserializeEnumerable<TEntity>(entitiesData, serializerOptions);

            foreach (var entity in entities)
            {
                var updEntity = await QueryHandlerHelper.AssignEntityReferenceProviders(queryService, _ServiceReference, entity);

                yield return updEntity.With(x => x.IsRetrievedFromStorage = true);
            }

            if (queryParameters?.Top is not null && entities.Count() > 0)
            {
                // increment the skip by the top and retrieve the next page...
                var newQueryParameters = queryParameters; // TODO - need to clone?

                if (newQueryParameters.Skip is null)
                    newQueryParameters.Skip = 0;

                newQueryParameters.Skip += newQueryParameters.Top;

                await foreach (var i in Get(queryService, newQueryParameters))
                {
                    yield return i;
                }
            }
        }

        #endregion

    }

}
