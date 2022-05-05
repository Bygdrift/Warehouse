using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Bygdrift.Warehouse.AppBaseTools
{
    internal class KeyVault
    {
        private List<SecretProperties> _secretProperties;
        private SecretClient _secretClient;

        public KeyVault(AppBase app) => App = app;

        public AppBase App { get; }

        public SecretClient SecretClient
        {
            get
            {
                if (_secretClient == null)  //Inspiration: https://stackoverflow.com/questions/43722030/how-to-get-connection-string-out-of-azure-keyvault/43747891
                {
                    var vaultUri = App.Config["VaultUri"];
                    if (vaultUri != null)
                        _secretClient = new SecretClient(vaultUri: new Uri(vaultUri), credential: new DefaultAzureCredential());
                    else if (!App.IsRunningLocal)
                        throw new Exception("'VaultUri' is not in appSettings. The module has been stopped.");
                }
                return _secretClient;
            }
        }

        public List<SecretProperties> SecretProperties
        {
            get
            {
                if (_secretProperties == null && SecretClient != null)
                    try
                    {
                        _secretProperties = SecretClient.GetPropertiesOfSecrets().ToList();
                    }
                    catch (AggregateException e)
                    {
                        throw new Exception($"There is an error that indicates that the vaultUri '{SecretClient.VaultUri}' doesn't exsist in Azure", e);
                    }
                    catch (Azure.RequestFailedException e)
                    {
                        if (e.Status == 401)
                        {
                            if (App.IsRunningLocal)
                                throw new Exception($"You are running local and has set the VaultUri to '{SecretClient.VaultUri}' in apppSettings, so Warehouse is trying to fetch secrets from the vault, but the system does not have authorized access.\n. If running from Visual Studio, you must be logged in with an AAD user from the Azure tenant.", e);
                            else
                                throw new Exception($"The system does not have authorized access to Azure keyvault: {SecretClient.VaultUri}.", e);
                        }
                        else
                            throw new Exception($"Unknown request error", e);
                    }

                return _secretProperties;
            }
        }

        internal string GetSecret(string secretName)
        {
            var vaultUri = App.Config["VaultUri"];
            if (App.IsRunningLocal && string.IsNullOrEmpty(vaultUri))  //Then local and the setting vaultUri has not been set:
            {
                var res = App.Config[secretName];
                if (res != null)
                    return res;

                //Look if the secret is saved as a normal setting. And thats okay because there cannot be a secret and a setting with the same name
                //So if "Secret--Module--DataLake", then look if it is saved as a setting: "DataLake":
                return App.Config[secretName.Split("--").Last()];
            }

            if (SecretProperties != null && SecretProperties.Any(o => o.Name == secretName))
            {
                var res = SecretClient.GetSecret(secretName);
                return res.Value.Value;
            }

            return null;
        }
    }
}
