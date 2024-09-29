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

using Sidub.Platform.Core;
using Sidub.Platform.Core.Entity;
using Sidub.Platform.Core.Entity.Relations;

#endregion

namespace Sidub.Platform.Storage.Handlers.Gremlin
{

    /// <summary>
    /// Helper class for building Gremlin queries.
    /// </summary>
    internal static class GremlinQueryHelper
    {

        #region Internal static methods

        /// <summary>
        /// Builds the Gremlin query for a given entity relation.
        /// </summary>
        /// <param name="relation">The entity relation.</param>
        /// <returns>The Gremlin query string.</returns>
        internal static string BuildRelationQuery(IEntityRelation relation)
        {
            var relatedKeys = EntityTypeHelper.GetEntityFields(relation, EntityFieldType.Keys).Select(x => x.FieldName);

            if (EntityTypeHelper.IsRelationshipAbstract(relation))
            {
                var keys = relatedKeys.ToList();
                keys.Add(TypeDiscriminatorEntityField.Instance.FieldName);
                relatedKeys = keys;
            }

            var relationString = ".by(";
            relationString += $"outE('{relation.Name}').inV()";
            relationString += $".project('{string.Join("','", relatedKeys)}')";
            relationString += string.Join(string.Empty, relatedKeys.Select(x => $".by(values('{x}'))"));

            if (relation.IsEnumerableRelation)
                relationString += ".fold()";
            else
                relationString += ".fold().coalesce(unfold(), constant('!dbNull'))";

            relationString += ".fold())";

            return relationString;
        }

        /// <summary>
        /// Wraps the given value in a Gremlin-compatible string representation.
        /// </summary>
        /// <param name="value">The value to wrap.</param>
        /// <returns>The wrapped value as a string.</returns>
        internal static string WrapGremlinValue(object? value)
        {
            if (value is int || value is uint || value is long || value is ulong || value is decimal || value is float)
                return value.ToString() ?? "";
            else if (value is bool)
                return value.ToString() ?? false.ToString();
            else if (value is DateTime valueDateTime)
                return $"'{valueDateTime.ToUniversalTime().ToString("o")}'";

            return $"'{value ?? ""}'";
        }

        #endregion

    }

}
