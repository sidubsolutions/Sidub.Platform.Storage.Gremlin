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

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Sidub.Platform.Authentication;
using Sidub.Platform.Filter;
using Sidub.Platform.Storage.Handlers;
using Sidub.Platform.Storage.Handlers.Gremlin.Factories;
using Sidub.Platform.Storage.Services;
using Sidub.Platform.Storage.Services.Gremlin;

#endregion

namespace Sidub.Platform.Storage
{

    /// <summary>
    /// Provides extension methods for configuring services.
    /// </summary>
    public static class ServiceCollectionExtension
    {

        #region Extension methods

        /// <summary>
        /// Adds Sidub storage services for Gremlin to the specified <see cref="IServiceCollection"/>.
        /// </summary>
        /// <param name="services">The <see cref="IServiceCollection"/> to add the services to.</param>
        /// <returns>The modified <see cref="IServiceCollection"/>.</returns>
        public static IServiceCollection AddSidubStorageForGremlin(
            this IServiceCollection services)
        {
            services.AddSidubAuthentication();
            services.AddSidubStorage();
            services.AddSidubFilter(FilterParserType.Gremlin)
                .AddScoped<GremlinDataProviderService>()
                .AddTransient<IDataHandlerService, GremlinDataHandlerService>();

            services.AddMemoryCache();

            services.TryAddEnumerable(ServiceDescriptor.Transient<ICommandHandlerFactory<GremlinStorageConnector>, EntitySaveRelationCommandHandlerFactory>());
            services.TryAddEnumerable(ServiceDescriptor.Transient<ICommandHandlerFactory<GremlinStorageConnector>, DeleteEntityCommandHandlerFactory>());

            return services;
        }

        #endregion

    }
}
