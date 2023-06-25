using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Bygdrift.Warehouse
{
    /// <summary>The key vault</summary>
    public class KeyVault
    {
        private List<SecretProperties> _secretProperties;
        private SecretClient _secretClient;
        private AppBase app;

        /// <summary>Init</summary>
        public KeyVault(AppBase app) => this.app = app;

        /// <summary>
        /// The SecretClient provides synchronous and asynchronous methods to manage <see cref="KeyVaultSecret"/> in the Azure Key Vault. The client
        /// supports creating, retrieving, updating, deleting, purging, backing up, restoring, and listing <see cref="KeyVaultSecret"/>.
        /// The client also supports listing <see cref="DeletedSecret"/> for a soft-delete enabled Azure Key Vault.
        /// </summary>
        public SecretClient SecretClient
        {
            get
            {
                if (_secretClient == null)  //Inspiration: https://stackoverflow.com/questions/43722030/how-to-get-connection-string-out-of-azure-keyvault/43747891
                {
                    var vaultUri = app.Config["VaultUri"];
                    if (vaultUri != null)
                        try
                        {
                            _secretClient = new SecretClient(vaultUri: new Uri(vaultUri), credential: new DefaultAzureCredential());
                        }
                        catch (Exception e)
                        {
                            throw new Exception($"The 'VaultUri': '{vaultUri}' from appSettings, gave this error: {e.Message}.");
                        }
                    else if (!app.IsRunningLocal)
                        throw new Exception("'VaultUri' is not in appSettings. The module has been stopped.");
                }
                return _secretClient;
            }
        }

        private List<SecretProperties> SecretProperties
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
                            if (app.IsRunningLocal)
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

        /// <summary>Get a secret</summary>
        public string GetSecret(string secretName)
        {
            var vaultUri = app.Config["VaultUri"];
            if (app.IsRunningLocal && string.IsNullOrEmpty(vaultUri))  //Then local and the setting vaultUri has not been set:
            {
                var res = app.Config[secretName];
                if (res != null)
                    return res;

                //Look if the secret is saved as a normal setting. And thats okay because there cannot be a secret and a setting with the same name
                //So if "Secret--Module--DataLake", then look if it is saved as a setting: "DataLake":
                return app.Config[secretName.Split("--").Last()];
            }

            if (SecretProperties != null && SecretProperties.Any(o => o.Name == secretName))
            {
                var res = SecretClient.GetSecret(secretName);
                return res.Value.Value;
            }

            return null;
        }

        /// <summary>Get all secrets</summary>
        public Dictionary<string, object> GetSecrets()
        {
            var res = new Dictionary<string, object>();
            var vaultUri = app.Config["VaultUri"];
            if (app.IsRunningLocal && string.IsNullOrEmpty(vaultUri))  //Then local and the setting vaultUri has not been set:
            {
                Microsoft.Extensions.Configuration.IConfigurationRoot g = app.Config;
                var data = g.GetChildren().Where(o=> o.Key.StartsWith("Secret--"));
                if(data.Any())
                    foreach (var item in data)
                        res.Add(item.Key, item.Value);
            }
            else if (SecretProperties != null)
                foreach (var item in SecretProperties)
                    res.Add(item.Name, SecretClient.GetSecret(item.Name));

            return res;
        }
    }
}
