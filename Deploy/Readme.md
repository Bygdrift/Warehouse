# Use ARM templates to setup and maintain your Warehouse

There is a lot going on in the setup of the warehouse environment on Azure, and to simplify that task, there will be written an Azure Resource Management template for each module so it becomes easy to install.

## Prerequisites

If you don't have an Azure subscription, create a free [account](https://azure.microsoft.com/free/?ref=microsoft.com&utm_source=microsoft.com&utm_medium=docs&utm_campaign=visualstudio) before you begin.

## Videos

2022-01-28: [How to setup the basic Bygdrift Warehouse (in Danish)](https://www.youtube.com/watch?v=6aR39glybhg)

## Setup the Warehouse environment with the portal:

[![Deploy To Azure](https://raw.githubusercontent.com/Bygdrift/Warehouse/master/Docs/Images/deploytoazureButton.svg)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FBygdrift%2FWarehouse%2Fmaster%2FDeploy%2FWarehouse_ARM.json)
[![Visualize](https://raw.githubusercontent.com/Bygdrift/Warehouse/master/Docs/Images/visualizebutton.svg)](http://armviz.io/#/?load=https%3A%2F%2Fraw.githubusercontent.com%2FBygdrift%2FWarehouse%2Fmaster%2FDeploy%2FWarehouse_ARM.json)

This will setup the warehouse environment and you will have to do it as the first thing. Then you can install the warehouse modules you like.

This deployment will setup a storage account for the apps, a storage account as a data lake, an Application Insight, a SQL server and a SQL database.

## Setup the environment with Azure CLI

You can also run the ARM from PowerShell.

Either run the PowerShell from computer by installing [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli), or use the [Azure Cloud Shell](https://shell.azure.com/bash) from the Azure portal. This instruction will focus on the run from a computer.

Download [warehouse_ARM.parameters.json](https://raw.githubusercontent.com/Bygdrift/Warehouse/master/Deploy/Warehouse_ARM.parameters.json) to a folder and carefully fill in each variable.

Download [warehouse_ARM.json](https://raw.githubusercontent.com/Bygdrift/Warehouse/master/Deploy/Warehouse_ARM.json) to the same folder.

Login to azure: `az login`.

If you don't have a resource group, then run the PowerShell-command: `az group create -g <resourceGroupName> -l westeurope`

And then: `az deployment group create -g <resourceGroupName> --template-file ./Warehouse_ARM.json --parameters ./Warehouse_ARM.parameters.json`

In both commands, replace `<resourceGroupName>` with actual name.

I personally prefer to use CLI so I can collect all parameters into one json.

