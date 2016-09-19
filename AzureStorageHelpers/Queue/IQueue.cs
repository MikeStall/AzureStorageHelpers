using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // Abstraction over an Azure Queue
    // QueueName rules: 3-63 lowercase alphanumeric and dash 
    public interface IQueue<TMessage>
    {
        Task EnqueueAsync(TMessage m, TimeSpan? invisibilityDelay = null);
    }   
}
