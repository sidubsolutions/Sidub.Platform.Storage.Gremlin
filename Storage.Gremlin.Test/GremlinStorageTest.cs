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

using Microsoft.Extensions.DependencyInjection;
using Sidub.Platform.Authentication;
using Sidub.Platform.Authentication.Credentials;
using Sidub.Platform.Core;
using Sidub.Platform.Core.Entity.Relations;
using Sidub.Platform.Core.Services;
using Sidub.Platform.Storage.Commands;
using Sidub.Platform.Storage.Services;
using Sidub.Platform.Storage.Test.Models;
using Sidub.Platform.Storage.Test.Models.Gremlin;

namespace Sidub.Platform.Storage.Test
{
    [TestClass]
    public class GremlinStorageTest
    {

        private readonly IServiceRegistry _entityMetadataService;
        private readonly IQueryService _queryService;

        public GremlinStorageTest()
        {
            // initialize dependency injection environment...
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSidubPlatform(serviceProvider =>
            {
                var metadataService = new InMemoryServiceRegistry();

                var gremlinUrl = Environment.GetEnvironmentVariable("GREMLIN_URL");
                var gremlinKey = Environment.GetEnvironmentVariable("GREMLIN_KEY");

                if (gremlinUrl is null)
                    throw new Exception("Gremlin URL not set.");

                if (gremlinKey is null)
                    throw new Exception("Gremlin key not set.");

                var dataService = new StorageServiceReference("UnitTests");
                var dataServiceConnector = new GremlinStorageConnector(gremlinUrl, "Sandbox", "UnitTests", "partitionId");
                var authenticationService = new AuthenticationServiceReference("UnitTestsAuth");
                var authenticationCredential = new GremlinPasswordCredential(gremlinKey);

                metadataService.RegisterServiceReference(dataService, dataServiceConnector);
                metadataService.RegisterServiceReference(authenticationService, authenticationCredential, dataService);

                return metadataService;
            });

            serviceCollection.AddSidubAuthenticationForGremlin();
            serviceCollection.AddSidubStorageForGremlin();
            serviceCollection.AddLogging();

            var serviceProvider = serviceCollection.BuildServiceProvider();

            _entityMetadataService = serviceProvider.GetService<IServiceRegistry>() ?? throw new Exception("Entity metadata service not initialized.");
            _queryService = serviceProvider.GetService<IQueryService>() ?? throw new Exception("Query service not initialized.");

            // register platform module...

        }

        [TestMethod]
        public async Task GremlinStorageTest_DateTimeTest01()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            var widget = new Widget01
            {
                Id = Guid.NewGuid(),
                Description = "Widget 01",
                CreatedOn = DateTime.UtcNow
            };

            var saveCommand = new SaveEntityCommand<Widget01>(widget);
            var saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsTrue(saveResult.IsSuccessful);

            var query = new Widget01Query(widget.Id);
            var result = await _queryService.Execute(module, query);

            if (result is null)
                Assert.Fail("Null object encountered in retrieval query.");

            Assert.AreEqual(widget.CreatedOn, result.CreatedOn);
        }

        [TestMethod]
        public async Task GremlinStorageTest_SaveAndDeleteData01()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            var category = new ProductDetail
            {
                ProductId = 0,
                Details = "Milk"
            };

            var saveCommand = new SaveEntityCommand<ProductDetail>(category);
            var saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsNotNull(saveResult);
            Assert.IsNotNull(saveResult.Result);
            Assert.AreEqual("Milk", saveResult.Result.Details);

            var query = new ProductDetailQuery() { Name = "Milk" };
            var result = await _queryService.Execute(module, query);

            Assert.IsNotNull(result);

            Assert.AreEqual("Milk", result.Details);

            var deleteCommand = new DeleteEntityCommand<ProductDetail>(result);
            var deleteResult = await _queryService.Execute(module, deleteCommand);
            Assert.IsNotNull(deleteResult);

            result = await _queryService.Execute(module, query);

            Assert.IsNull(result);

        }

        [TestMethod]
        public async Task GremlinStorageTest_SaveAndQueryData01()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            var user = new GremlinUser
            {
                UserId = "john.doe",
                Name = "john doe"
            };

            var saveCommand = new SaveEntityCommand<GremlinUser>(user);
            var saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsNotNull(saveResult);
            Assert.IsNotNull(saveResult.Result);
            Assert.IsTrue(saveResult.IsSuccessful);
            Assert.AreEqual("john.doe", saveResult.Result.UserId);
            Assert.AreEqual("john doe", saveResult.Result.Name);

            var query = new GremlinUserByUsernameQuery
            {
                Username = "john.doe"
            };

            user = _queryService.Execute(module, query).Result!;

            Assert.IsNotNull(user);
            Assert.AreEqual("john.doe", user.UserId);
            Assert.AreEqual("john doe", user.Name);

            user.Name = "john z. doe";

            saveCommand = new SaveEntityCommand<GremlinUser>(user);
            saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsNotNull(saveResult);
            Assert.IsNotNull(saveResult.Result);
            Assert.IsTrue(saveResult.IsSuccessful);
            Assert.AreEqual("john.doe", saveResult.Result.UserId);
            Assert.AreEqual("john z. doe", saveResult.Result.Name);

            user = _queryService.Execute(module, query).Result!;

            Assert.IsNotNull(user);
            Assert.AreEqual("john.doe", user.UserId);
            Assert.AreEqual("john z. doe", user.Name);
        }

        [TestMethod]
        public async Task GremlinStorageTest_SaveAndQueryData02()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            var category = new ProductDetail
            {
                ProductId = 0,
                Details = "Milk"
            };

            var saveCommand = new SaveEntityCommand<ProductDetail>(category);
            var saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsNotNull(saveResult);
            Assert.IsNotNull(saveResult.Result);
            Assert.AreEqual("Milk", saveResult.Result.Details);

            var query = new ProductDetailQuery() { Name = "Milk" };
            var result = await _queryService.Execute(module, query);

            Assert.IsNotNull(result);

            Assert.AreEqual("Milk", result.Details);
        }

        [TestMethod]
        public async Task GremlinStorageTest_SaveAndQueryData03()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            var category = new ProductDetail
            {
                Details = "Milk"
            };

            var saveCommand = new SaveEntityCommand<ProductDetail>(category);
            var saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsNotNull(saveResult);
            Assert.IsNotNull(saveResult.Result);
            Assert.AreEqual("Milk", saveResult.Result.Details);

            category = new ProductDetail
            {
                Details = "Juice"
            };

            saveCommand = new SaveEntityCommand<ProductDetail>(category);
            saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsNotNull(saveResult);
            Assert.IsNotNull(saveResult.Result);
            Assert.AreEqual("Juice", saveResult.Result.Details);

            category = new ProductDetail
            {
                Details = "Eggs"
            };

            saveCommand = new SaveEntityCommand<ProductDetail>(category);
            saveResult = await _queryService.Execute(module, saveCommand);

            Assert.IsNotNull(saveResult);
            Assert.IsNotNull(saveResult.Result);
            Assert.AreEqual("Eggs", saveResult.Result.Details);

            var query = new AllProductDetailsQuery();
            var result = await _queryService.Execute(module, query).ToListAsync();

            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task QueryWithRecordRelationsTest()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            MenuItemType menuItemType = new MenuItemType()
            {
                Id = Guid.NewGuid(),
                ActionType = "someActionType"
            };

            var saveMenuItemType = new SaveEntityCommand<MenuItemType>(menuItemType);
            var saveMenuItemTypeResult = await _queryService.Execute(module, saveMenuItemType);

            Assert.IsNotNull(saveMenuItemTypeResult);
            Assert.IsTrue(saveMenuItemTypeResult.IsSuccessful);

            menuItemType = saveMenuItemTypeResult.Result
                ?? throw new Exception("Null save result encountered.");

            var menuItem = new MenuItem
            {
                Id = Guid.NewGuid(),
                Type = EntityReference.Create(menuItemType),
                Name = "someName",
                Description = "someDescription"
            };

            var saveMenuItem = new SaveEntityCommand<MenuItem>(menuItem);
            var saveMenuItemResult = await _queryService.Execute(module, saveMenuItem);

            Assert.IsNotNull(saveMenuItemResult);
            Assert.IsTrue(saveMenuItemResult.IsSuccessful);

            var menuItemQuery = new MenuItemByIdQuery(saveMenuItemResult.Result.Id);
            var menuItemResult = await _queryService.Execute(module, menuItemQuery);

            Assert.IsNotNull(menuItemResult);

            var menuItemTypeResult = await menuItemResult.Type.Get();

            Assert.IsNotNull(menuItemTypeResult);

        }

        [TestMethod]
        public async Task RemoveRecordRelationsTest()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            var menuItemType = new MenuItemType()
            {
                Id = Guid.NewGuid(),
                ActionType = "someActionType"
            };

            var saveMenuItemType = new SaveEntityCommand<MenuItemType>(menuItemType);
            var saveMenuItemTypeResult = await _queryService.Execute(module, saveMenuItemType);

            Assert.IsNotNull(saveMenuItemTypeResult);
            Assert.IsTrue(saveMenuItemTypeResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemTypeResult.Result);

            menuItemType = saveMenuItemTypeResult.Result;

            var menuItem = new MenuItem
            {
                Id = Guid.NewGuid(),
                Type = EntityReference.Create(menuItemType),
                Name = "someName",
                Description = "someDescription"
            };

            var saveMenuItem = new SaveEntityCommand<MenuItem>(menuItem);
            var saveMenuItemResult = await _queryService.Execute(module, saveMenuItem);

            Assert.IsNotNull(saveMenuItemResult);
            Assert.IsTrue(saveMenuItemResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemResult.Result);

            menuItem = saveMenuItemResult.Result;

            var menuItemQuery = new MenuItemByIdQuery(menuItem.Id);
            var menuItemResult = await _queryService.Execute(module, menuItemQuery);

            Assert.IsNotNull(menuItemResult);

            var menuItemTypeResult = await menuItemResult.Type.Get();

            Assert.IsNotNull(menuItemTypeResult);

            menuItem.Type.Clear();

            saveMenuItem = new SaveEntityCommand<MenuItem>(menuItem);
            saveMenuItemResult = await _queryService.Execute(module, saveMenuItem);

            Assert.IsNotNull(saveMenuItemResult);
            Assert.IsTrue(saveMenuItemResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemResult.Result);

            menuItem = saveMenuItemResult.Result;

            menuItemQuery = new MenuItemByIdQuery(menuItem.Id);
            menuItemResult = await _queryService.Execute(module, menuItemQuery);

            Assert.IsNotNull(menuItemResult);
            Assert.IsFalse(menuItemResult.Type.HasValue());
        }

        [TestMethod]
        public async Task QueryWithEnumerableRelationsTest()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            // initialize and save the menu item type #1...
            var menuItemType = new MenuItemType()
            {
                Id = Guid.NewGuid(),
                ActionType = "someActionType"
            };

            var saveMenuItemType = new SaveEntityCommand<MenuItemType>(menuItemType);
            var saveMenuItemTypeResult = await _queryService.Execute(module, saveMenuItemType);

            Assert.IsNotNull(saveMenuItemTypeResult);
            Assert.IsTrue(saveMenuItemTypeResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemTypeResult.Result);

            menuItemType = saveMenuItemTypeResult.Result;

            // initialize and save the menu item #1...
            var menuItem = new MenuItem
            {
                Id = Guid.NewGuid(),
                Type = EntityReference.Create(menuItemType),
                Name = "someName",
                Description = "someDescription"
            };

            var saveMenuItem = new SaveEntityCommand<MenuItem>(menuItem);
            var saveMenuItemResult = await _queryService.Execute(module, saveMenuItem);

            Assert.IsNotNull(saveMenuItemResult);
            Assert.IsTrue(saveMenuItemResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemResult.Result);

            menuItem = saveMenuItemResult.Result;

            // initialize and save the menu item type #2...
            var menuItemType2 = new MenuItemType()
            {
                Id = Guid.NewGuid(),
                ActionType = "someActionType2"
            };

            saveMenuItemType = new SaveEntityCommand<MenuItemType>(menuItemType2);
            saveMenuItemTypeResult = await _queryService.Execute(module, saveMenuItemType);

            Assert.IsNotNull(saveMenuItemTypeResult);
            Assert.IsTrue(saveMenuItemTypeResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemTypeResult.Result);

            menuItemType2 = saveMenuItemTypeResult.Result;

            // initialize and save the menu item #2...
            var menuItem2 = new MenuItem
            {
                Id = Guid.NewGuid(),
                Type = EntityReference.Create(menuItemType2),
                Name = "someName2",
                Description = "someDescription2"
            };

            saveMenuItem = new SaveEntityCommand<MenuItem>(menuItem2);
            saveMenuItemResult = await _queryService.Execute(module, saveMenuItem);

            Assert.IsNotNull(saveMenuItemResult);
            Assert.IsTrue(saveMenuItemResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemResult.Result);

            menuItem2 = saveMenuItemResult.Result;

            var menuItemGroup = new MenuItemGroup()
            {
                Id = Guid.NewGuid(),
                Items = new EntityReferenceList<MenuItem>()
                {
                    menuItem,
                    menuItem2
                }
            };

            var saveMenuItemGroup = new SaveEntityCommand<MenuItemGroup>(menuItemGroup);
            var saveMenuItemGroupResult = await _queryService.Execute(module, saveMenuItemGroup);

            Assert.IsNotNull(saveMenuItemGroupResult);
            Assert.IsTrue(saveMenuItemGroupResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemGroupResult.Result);

            menuItemGroup = saveMenuItemGroupResult.Result;

            var menuItemGroupQuery = new MenuItemGroupByIdQuery(menuItemGroup.Id);
            var menuItemGroupResult = await _queryService.Execute(module, menuItemGroupQuery);

            Assert.IsNotNull(menuItemGroupResult);
            Assert.IsNotNull(menuItemGroupResult.Items);
            Assert.AreEqual(2, menuItemGroupResult.Items.Count);

            var firstItem = await menuItemGroupResult.Items[0].Get();
            var secondItem = await menuItemGroupResult.Items[1].Get();

            Assert.IsNotNull(firstItem);
            Assert.IsNotNull(secondItem);

        }

        [TestMethod]
        public async Task RemoveEnumerableRelationsTest()
        {
            var module = _entityMetadataService.GetServiceReference<StorageServiceReference>("UnitTests")
                ?? throw new Exception("Unit test storage module not found.");

            // initialize and save the menu item type #1...
            var menuItemType = new MenuItemType()
            {
                Id = Guid.NewGuid(),
                ActionType = "someActionType"
            };

            var saveMenuItemType = new SaveEntityCommand<MenuItemType>(menuItemType);
            var saveMenuItemTypeResult = await _queryService.Execute(module, saveMenuItemType);

            Assert.IsNotNull(saveMenuItemTypeResult);
            Assert.IsTrue(saveMenuItemTypeResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemTypeResult.Result);

            menuItemType = saveMenuItemTypeResult.Result;

            // initialize and save the menu item #1...
            var menuItem = new MenuItem
            {
                Id = Guid.NewGuid(),
                Type = EntityReference.Create(menuItemType),
                Name = "someName",
                Description = "someDescription"
            };

            var saveMenuItem = new SaveEntityCommand<MenuItem>(menuItem);
            var saveMenuItemResult = await _queryService.Execute(module, saveMenuItem);

            Assert.IsNotNull(saveMenuItemResult);
            Assert.IsTrue(saveMenuItemResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemResult.Result);

            menuItem = saveMenuItemResult.Result;

            // initialize and save the menu item type #2...
            var menuItemType2 = new MenuItemType()
            {
                Id = Guid.NewGuid(),
                ActionType = "someActionType2"
            };

            saveMenuItemType = new SaveEntityCommand<MenuItemType>(menuItemType2);
            saveMenuItemTypeResult = await _queryService.Execute(module, saveMenuItemType);

            Assert.IsNotNull(saveMenuItemTypeResult);
            Assert.IsTrue(saveMenuItemTypeResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemTypeResult.Result);

            menuItemType2 = saveMenuItemTypeResult.Result;

            // initialize and save the menu item #2...
            var menuItem2 = new MenuItem
            {
                Id = Guid.NewGuid(),
                Type = EntityReference.Create(menuItemType2),
                Name = "someName2",
                Description = "someDescription2"
            };

            saveMenuItem = new SaveEntityCommand<MenuItem>(menuItem2);
            saveMenuItemResult = await _queryService.Execute(module, saveMenuItem);

            Assert.IsNotNull(saveMenuItemResult);
            Assert.IsTrue(saveMenuItemResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemResult.Result);

            menuItem2 = saveMenuItemResult.Result;

            var menuItemGroup = new MenuItemGroup()
            {
                Id = Guid.NewGuid(),
                Items = new EntityReferenceList<MenuItem>()
                {
                    menuItem,
                    menuItem2
                }
            };

            var saveMenuItemGroup = new SaveEntityCommand<MenuItemGroup>(menuItemGroup);
            var saveMenuItemGroupResult = await _queryService.Execute(module, saveMenuItemGroup);

            Assert.IsNotNull(saveMenuItemGroupResult);
            Assert.IsTrue(saveMenuItemGroupResult.IsSuccessful);
            Assert.IsNotNull(saveMenuItemGroupResult.Result);

            menuItemGroup = saveMenuItemGroupResult.Result;

            var menuItemGroupQuery = new MenuItemGroupByIdQuery(menuItemGroup.Id);
            var menuItemGroupResult = await _queryService.Execute(module, menuItemGroupQuery);

            Assert.IsNotNull(menuItemGroupResult);
            Assert.IsNotNull(menuItemGroupResult.Items);
            Assert.AreEqual(2, menuItemGroupResult.Items.Count);

            var firstItemRef = menuItemGroupResult.Items[0];
            var secondItemRef = menuItemGroupResult.Items[1];
            var firstItem = await firstItemRef.Get();
            var secondItem = await secondItemRef.Get();

            Assert.IsNotNull(firstItem);
            Assert.IsNotNull(secondItem);

            // this is removed for future consideration / implementation...
            //// ensure we are unable to use the clear method on a reference contained within an enumerable reference list...
            ////Assert.ThrowsException<Exception>(() => menuItemGroupResult.Items[0].Clear());

            // remove the first item from the list and save it...
            menuItemGroupResult.Items.Remove(firstItemRef);

            saveMenuItemGroup = new SaveEntityCommand<MenuItemGroup>(menuItemGroupResult);
            saveMenuItemGroupResult = await _queryService.Execute(module, saveMenuItemGroup);

            Assert.IsTrue(saveMenuItemGroupResult.IsSuccessful);

            menuItemGroupResult = saveMenuItemGroupResult.Result;

            Assert.IsNotNull(menuItemGroupResult);
            Assert.AreEqual(1, menuItemGroupResult.Items?.Count);

        }

    }
}
