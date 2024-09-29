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

using Sidub.Platform.Core.Attributes;
using Sidub.Platform.Core.Serializers;
using Sidub.Platform.Storage.Connectors;

#endregion

namespace Sidub.Platform.Storage
{

    /// <summary>
    /// Represents a Gremlin storage connector.
    /// </summary>
    public class GremlinStorageConnector : IStorageConnector
    {

        #region Public properties

        /// <summary>
        /// Gets the serialization language type of the storage connector.
        /// </summary>
        public SerializationLanguageType SerializationLanguage => SerializationLanguageType.Json;

        /// <summary>
        /// Gets or sets the database URI.
        /// </summary>
        [EntityField<string>("DatabaseUri")]
        public string DatabaseUri { get; set; }

        /// <summary>
        /// Gets or sets the database name.
        /// </summary>
        [EntityField<string>("DatabaseName")]
        public string DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the graph name.
        /// </summary>
        [EntityField<string>("GraphName")]
        public string GraphName { get; set; }

        /// <summary>
        /// Gets or sets the primary key field name.
        /// </summary>
        [EntityField<string>("PrimaryKeyFieldName")]
        public string PrimaryKeyFieldName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the partition key field name.
        /// </summary>
        [EntityField<string>("PartitionKeyFieldName")]
        public string PartitionKeyFieldName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets a value indicating whether the connector is retrieved from storage.
        /// </summary>
        public bool IsRetrievedFromStorage { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="GremlinStorageConnector"/> class.
        /// </summary>
        public GremlinStorageConnector()
        {
            DatabaseUri = string.Empty;
            DatabaseName = string.Empty;
            GraphName = string.Empty;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="GremlinStorageConnector"/> class with the specified database URI, database name, and graph name.
        /// </summary>
        /// <param name="databaseUri">The database URI.</param>
        /// <param name="databaseName">The database name.</param>
        /// <param name="graphName">The graph name.</param>
        public GremlinStorageConnector(string databaseUri, string databaseName, string graphName, string partitionKeyFieldName)
        {
            DatabaseUri = databaseUri;
            DatabaseName = databaseName;
            GraphName = graphName;
            PartitionKeyFieldName = partitionKeyFieldName;
        }

        #endregion

        #region Public methods

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return DatabaseUri.GetHashCode() ^ DatabaseName.GetHashCode() ^ GraphName.GetHashCode();
        }

        /// <inheritdoc/>
        public override bool Equals(object? obj)
        {
            if (obj is GremlinStorageConnector gremlinConnector)
            {
                return DatabaseUri == gremlinConnector.DatabaseUri
                    && DatabaseName == gremlinConnector.DatabaseName
                    && GraphName == gremlinConnector.GraphName;
            }

            return false;
        }

        #endregion
    }

}
