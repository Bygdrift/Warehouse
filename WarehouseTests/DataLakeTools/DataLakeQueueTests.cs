using Azure.Storage.Queues;
using Bygdrift.DataLakeTools;
using Bygdrift.Warehouse;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tests.DataLakeTools
{
    /// <summary>
    /// TODO: Test at sende flere filer med samme filePath på en gang og sørg for at håndtere når den brækker ned!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    /// </summary>

    [TestClass]
    public class DataLakeSetTests
    {
        private static AppBase app = new();

        [TestMethod]
        public async Task Queue()  //There will come errors on concurrent readings. Make sure to get the errors fetched into the log.
        {
            await app.DataLakeQueue.DeleteMessagesAsync();

            var tasks = new List<string>();
            for (int i = 0; i < 1000; i++)
                tasks.Add("Besked " + i);

            await app.DataLakeQueue.AddMessagesAsync(tasks.ToArray());

            var counts = await app.DataLakeQueue.MessagesCountAsync();
            Assert.IsTrue(counts == 1000);

            var queues = await app.DataLakeQueue.GetMessagesAsync();
            await app.DataLakeQueue.DeleteMessagesAsync(queues);

            counts = await app.DataLakeQueue.MessagesCountAsync();
            Assert.IsTrue(counts == 0);
        }

        [TestMethod]
        public async Task QueueChangeName()  //There will come errors on concurrent readings. Make sure to get the errors fetched into the log.
        {
            await app.DataLakeQueue.DeleteMessagesAsync();

            await app.DataLakeQueue.AddMessageAsync("Message");

            app.DataLakeQueue.QueueName = "testName";

            await app.DataLakeQueue.AddMessageAsync("Message");

            var counts = await app.DataLakeQueue.MessagesCountAsync();
            Assert.IsTrue(counts == 1);

            await app.DataLakeQueue.DeleteMessagesAsync();

            app.DataLakeQueue.QueueName = null;

            await app.DataLakeQueue.DeleteQueueAsync();
        }
    }
}
