using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{

    // Real Azure Queue. 
    public class AzureQueue<TMessage> : IQueue<TMessage>
    {
        private readonly CloudQueue _queue;

        public AzureQueue(string accountConnectionString, string queueName)
        {
            var account = CloudStorageAccount.Parse(accountConnectionString);
            var client = account.CreateCloudQueueClient();
            var queue = client.GetQueueReference(queueName);

            _queue = queue;
            _queue.CreateIfNotExists();
        }

        public AzureQueue(CloudQueue queue)
        {
            _queue = queue;
            _queue.CreateIfNotExists();
        }

        public async Task EnqueueAsync(TMessage m, TimeSpan? invisibilityDelay)
        {
            string content = JsonConvert.SerializeObject(m, Formatting.Indented);
            CloudQueueMessage msg = new CloudQueueMessage(content);
            await _queue.AddMessageAsync(msg, null, invisibilityDelay, null, null);
        }
    }
}