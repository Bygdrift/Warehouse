# Warehouse

This solution makes it easy to write C# modules that fetches data from different systems, and pushes data into an Azure data lake, which may contain large amounts of data.

It also handles creation of Common Data Models, that enables integrations between data lake and Power BI, with Power BI Dataflows.

It is packed as a [nuget library](https://www.nuget.org/packages/Bygdrift.Warehouse).

Modules that uses this nuget library:

*   Ingest data from the FM-system: Dalux and up to a data lake: [Warehouse.Modules.DaluxFM](https://github.com/hillerod/Warehouse.Modules.DaluxFM).

[The license](LICENSE.md)
