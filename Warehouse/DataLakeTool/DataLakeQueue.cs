using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Bygdrift.Warehouse;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bygdrift.DataLakeTools
{
    /// <summary>
    /// A datalake working with messages
    /// </summary>
    public class DataLakeQueue
    {
        private static QueueClient _queueClient;

        /// <summary>
        /// Constructror for dataLake
        /// </summary>
        /// <param name="app">The AppBase</param>
        public DataLakeQueue(AppBase app) => App = app;

        /// <summary>
        /// The AppBase
        /// </summary>
        public AppBase App { get; }

        private string _queueName;

        /// <summary>
        /// By default the lowercased module name. If set, then the name is "moduleName-queueName" - all lower case.
        /// To go back to default name, just set the name to null.
        /// </summary>
        public string QueueName
        {
            get
            {
                return _queueName ??= App.ModuleName.ToLower();
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                    _queueName = App.ModuleName.ToLower();
                else
                {
                    if (!value.All(o => char.IsLetter(o)))
                        throw new Exception("The queueName, must only contain letters.");

                    _queueName = App.ModuleName.ToLower() + "-" + value.ToLower();
                }
                if(_queueClient != null)
                {
                    _queueClient = new QueueClient(App.DataLakeConnectionString, QueueName);
                    _queueClient.CreateIfNotExists();
                }
            }
        }

        /// <summary>
        /// The client. If QueueName is not set, then the default name is ModuleName
        /// </summary>
        public QueueClient QueueClient
        {
            get
            {
                if (_queueClient == null)
                {
                    _queueClient = new QueueClient(App.DataLakeConnectionString, QueueName);
                    _queueClient.CreateIfNotExists();
                }
                return _queueClient;
            }
        }

        /// <summary>
        /// Adds a message
        /// </summary>
        public async Task<SendReceipt> AddMessageAsync(string message)
        {
            return (await QueueClient.SendMessageAsync(message)).Value;
        }

        /// <summary>
        /// Add multiple messages
        /// </summary>
        public async Task<IEnumerable<SendReceipt>> AddMessagesAsync(string[] messages)
        {
            var tasks = new List<Task<Azure.Response<SendReceipt>>>();
            foreach (var item in messages)
                tasks.Add(QueueClient.SendMessageAsync(item));

            await Task.WhenAll(tasks);
            return tasks.Select(o => o.Result.Value);
        }

        /// <summary>
        /// Delete all messages
        /// </summary>
        public async Task DeleteMessagesAsync()
        {
            var messages = await GetMessagesAsync();
            await DeleteMessagesAsync(messages);
        }

        /// <summary>
        /// Delete a message
        /// </summary>
        public async Task DeleteMessageAsync(QueueMessage message)
        {
            await QueueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt);
        }

        /// <summary>
        /// Delete multiple messages
        /// </summary>
        public async Task DeleteMessagesAsync(IEnumerable<QueueMessage> messages)
        {
            if (messages == null)
                return;

            var tasks = new List<Task>();
            foreach (var message in messages)
                tasks.Add(QueueClient.DeleteMessageAsync(message.MessageId, message.PopReceipt));

            await Task.WhenAll(tasks);
        }

        /// <summary>
        /// Delete the message
        /// </summary>
        public async Task DeleteQueueAsync()
        {
           await QueueClient.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Get a message
        /// </summary>
        public async Task<QueueMessage> GetMessageAsync()
        {
            return await QueueClient.ReceiveMessageAsync();
        }

        /// <summary>
        /// Get multiple messages
        /// </summary>
        public async Task<IEnumerable<QueueMessage>> GetMessagesAsync(int? amount = null)
        {
            var tasks = new List<Task<Azure.Response<QueueMessage[]>>>();

            if (amount <= 32)
                return (await QueueClient.ReceiveMessagesAsync(amount)).Value;

            var messageCounts = (await QueueClient.GetPropertiesAsync()).Value.ApproximateMessagesCount;
            if (messageCounts == 0)
                return default;

            if (amount == null || messageCounts < amount)
                amount = messageCounts;

            var take = 32;  //32 is maximum for message to retrieve per call
            var calls = Math.Ceiling((double)amount / take);

            for (int i = 1; i <= calls; i++)
            {
                if (i == calls && i * take != amount)
                    take = (int)amount % take;

                tasks.Add(QueueClient.ReceiveMessagesAsync(take));
            }

            await Task.WhenAll(tasks);
            return tasks.SelectMany(o => o.Result.Value);
        }

        /// <summary>
        /// Peek a message so it doesn't turn invisible for 30 seconds
        /// </summary>
        public async Task<PeekedMessage> PeekMessageAsync()
        {
            return await QueueClient.PeekMessageAsync();
        }

        /// <summary>
        /// Peek multiple messages so it doesn't turn invisible for 30 seconds. Twice as fast as Getmessages but can't be deleted
        /// </summary>
        public async Task<IEnumerable<PeekedMessage>> PeekMessagesAsync(int? amount = null)
        {
            var tasks = new List<Task<Azure.Response<PeekedMessage[]>>>();

            if (amount <= 32)
                return (await QueueClient.PeekMessagesAsync(amount)).Value;

            var messageCounts = (await QueueClient.GetPropertiesAsync()).Value.ApproximateMessagesCount;
            if (messageCounts == 0)
                return default;

            if (amount == null || messageCounts < amount)
                amount = messageCounts;

            var take = 32;  //32 is maximum for message to retrieve per call
            var calls = Math.Ceiling((double)amount / take);

            for (int i = 1; i <= calls; i++)
            {
                if (i == calls && i * take != amount)
                    take = (int)amount % take;

                tasks.Add(QueueClient.PeekMessagesAsync(take));
            }

            await Task.WhenAll(tasks);
            return tasks.SelectMany(o => o.Result.Value);
        }

        /// <summary>
        /// Counts the amount of messages
        /// </summary>
        public async Task<int> MessagesCountAsync()
        {
            return (await QueueClient.GetPropertiesAsync()).Value.ApproximateMessagesCount;
        }
    }
}
