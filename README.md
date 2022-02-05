# Warehouse

## Introduction

This set of Warehouse tools, makes it easy to build modules in a standardized way, that collects data from a service, refine the data, and save it into a cheap data lake and/or database in Microsoft Azure.
![The flow](https://raw.githubusercontent.com/Bygdrift/Warehouse/master/Docs/Images/setup-in-azure.drawio.png)
 
It is available as a [NuGet package](https://www.nuget.org/packages/Bygdrift.Warehouse) 

If you want to try this warehouse, then start with an [installation of the warehouse on Azure](https://github.com/Bygdrift/Warehouse/blob/master/Deploy). Afterwards install the [Warehouse.Modules.Example](https://github.com/Bygdrift/Warehouse.Modules.Example). It is relatively simple to setup with the Azure Resource Management templates, that will guide you through the process.

## Videos

The Bygdrift Warehouse concept describe in one minute on Danish (*[here in English](https://youtu.be/J2vETtjk6kY)*):
<div align="left">
      <a href="https://www.youtube.com/watch?v=ZNsSg-msEiA">
         <img src="https://img.youtube.com/vi/ZNsSg-msEiA/0.jpg">
      </a>
</div>

Short video on how to setup a Bygdrift Warehouse and install the DaluxFM Module without deeper explanations (it's in English):
<div align="left">
      <a href="https://www.youtube.com/watch?v=ahREssLMLG0">
         <img src="https://img.youtube.com/vi/ahREssLMLG0/0.jpg">
      </a>
</div>

## Modules that uses this NuGet package right now:

*   A 'Hello world' example on how to build a module, that brings example-data in to a data lake in Azure: [Warehouse.Modules.Example](https://github.com/Bygdrift/Warehouse.Modules.Example)
*   Ingest data from the FM-system through their web service: Dalux: [Warehouse.Modules.DaluxFM](https://github.com/hillerod/Warehouse.Modules.DaluxFM)
*   Ingest data from the FM-system through their API: [Warehouse.Modules.DaluxFMApi](https://github.com/Bygdrift/Warehouse.Modules.DaluxFMApi)
*   Get data from Eloverblik - a Danish platform that shows power consumption: [Warehouse.Modules.Eloverblik](https://github.com/hillerod/Warehouse.Modules.Eloverblik)
*   Get data from Outlook Exchange on room bookings: [Warehouse.Modules.OutlookCalendar](https://github.com/hillerod/Warehouse.Modules.OutlookCalendar)

## Installation

All modules can be installed and facilitated with ARM templates(Azure Resource Management).Read more about the process here: [Use ARM templates to setup and maintain your Warehouse](https://github.com/Bygdrift/Warehouse/blob/master/Deploy).

## License

[The license](LICENSE.md)

# Updates

## 0.6.2

In 0.6.1, all user settings should have a prefix of 'Setting--'. That has been removed, so when upgrading from 0.6.1, then go to each module, like 'DaluxFM-{resourcegroup.Id()}' > Configuration, and remove the prefixes, so 'Setting--ScheduleImportEstatesAndAssets' becomes 'ScheduleImportEstatesAndAssets'. Secrets in key-vault, will continue to have prefixes like 'Secret--' and 'Secret--{ModuleName}--'.
