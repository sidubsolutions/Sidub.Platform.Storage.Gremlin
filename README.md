# Sidub Platform - Gremlin Storage

This repository contains the Gremlin storage module for the Sidub Platform. It
provides connectors and handlers that allow the storage framework to interact
with Gremlin graph databases.

## Main Components
This library simply provides the connectors and handlers for Gremlin data
services. The `Sidub.Platform.Authentication.Gremlin` library provides
authentication capabilities and credentials for this data service.

### Registering a Data Service
To connect to a Gremlin data source, register it as a metadata service using
the `StorageServiceReference` and `GremlinStorageConnector` classes.

```csharp
serviceCollection.AddSidubPlatform(serviceProvider =>
{
    var metadataService = new InMemoryServiceRegistry();

    var dataService = new StorageServiceReference("MyApi");
    var dataServiceConnector = new GremlinStorageConnector("xxx.gremlin.cosmos.azure.com", "DatabaseName", "GraphName", "partitionKey");
    var authenticationService = new AuthenticationServiceReference("MyApiCredential");
    var authenticationCredential = new GremlinPasswordCredential("xxx");

    metadataService.RegisterServiceReference(dataService, dataServiceConnector);
    metadataService.RegisterServiceReference(authenticationService, authenticationCredential, dataService);

    return metadataService;
});
```

To interact with the Gremlin data service, use any of the functionality defined
within the storage framework, simply passing the storage service reference
associated with the Gremlin connector.

## License
This project is dual-licensed under the AGPL v3 or a proprietary license. For
details, see [https://sidub.ca/licensing](https://sidub.ca/licensing) or the
LICENSE.txt file.