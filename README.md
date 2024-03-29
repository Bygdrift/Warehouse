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

## Contact

For information or consultant hours, please write to bygdrift@gmail.com.

# Updates
## 1.3.2
Breaking changes in the csvTool: The following methods has changed names: FromCsvFile => AddCsvFile, FromCsvStream => AddCsvStream, FromDataTable => AddDataTable, FromExcelFile => AddExcelFile, FromExcelStream => AddExcelStream, FromExpandoObjects => AddExpandoObjects, FromJson => AddJson.
Now they all can add data to an existing csv like:
```c#
Csv csv = new Csv("Id, Name").AddRows("A, Anders");
Csv csvIn = new Csv("Id, Name").AddRows("B, Bo");
csv.AddCsv(csvIn);
```
And csv equals:
| Id | Name   |
|----|--------|
| A  | Anders |
| B  | Bo     |


## 1.1.0
Decentralized this library, so it now refers to Nuget packages DataLakeTool, LogTool, MssqTool, CsvTool instead of having all this inside of one big library.
The mentined libraries are put out on GitHub with good documentation.
This summer, I have focused on the MssqlTool and added quite a lot of unit testing, to stabilize the transactions from CSV to database.

## 1.0.4
Added ability to handle Culture in CSV with new Csv().Culture();
Better to pinpoint the type of a string date in the CSV. So a column containing ex '2022-11-20' or '20-11-2022' and so on, are now better recognized.

## 1.0.3
Added option to import CSVFile with other delimiters than comma.

## 1.0.2
Update packages to newest .net6 version + minor updates

## 1.0.1
Added DataLakeQueue, so now it's easy to send, peek and receive queue messages from the Data Lake.

Added function to DataLake:
- Old data can easily be removed: DeleteDirectoriesOlderThanDaysAsync(string basePath, int equalOrolderThanDays)
- Get all directories: GetDirectories(string basePath)

Added two functions to CsvTools:
- A csv can be merged into another csv: FromCsv(Csv mergedCsv, bool createNewUniqueHeaderIfAlreadyExists)
- A column with the same data, can be added: AddColumn(string headerName, object value, bool createNewUniqueHeaderIfAlreadyExists)

## 1.0.0
Now so stable, that it's upgraded to production version.

## 0.7.0
Added function to Module.Settings, so its now poosible to add json with the attribute: [ConfigSetting(IsJson = true)].
Fixed error: When data is send to MSSQL like fx integer and later gets changed to decimal, there was an error, but it's fixed.

## 0.6.6
Added an update to CsvTools, so now it's possible to add a record by giving the header name instead of column number. The function: public Csv AddRecord(int row, string headerName, object value, bool createNewUniqueHeaderIfAlreadyExists = false)

## 0.6.3
Minor changes.

## 0.6.2
In 0.6.1, all user settings should have a prefix of 'Setting--'. That has been removed, so when upgrading from 0.6.1, then go to each module, like 'DaluxFM-{resourcegroup.Id()}' > Configuration, and remove the prefixes, so 'Setting--ScheduleImportEstatesAndAssets' becomes 'ScheduleImportEstatesAndAssets'. Secrets in key-vault, will continue to have prefixes like 'Secret--' and 'Secret--{ModuleName}--'.
