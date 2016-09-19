using Newtonsoft.Json;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // Simulate a queue locally on the file system. 
    public class FileQueue<TMessage> : IQueue<TMessage>
    {
        private readonly string _root;
        public FileQueue(string root, string queueName)
        {
            _root = Path.Combine(root, "queue", queueName);
            Directory.CreateDirectory(_root);
        }

        public async Task EnqueueAsync(TMessage m, TimeSpan? invisibilityDelay)
        {
            string content = JsonConvert.SerializeObject(m);

            var id = Guid.NewGuid().ToString();
            string path = Path.Combine(_root, id);
            File.WriteAllText(path, content);
        }

        // Wait on the queue, invoke worker on each message queued. 
        // Return if no work available. 
        public async Task PollAsync(Func<TMessage, Task> worker, CancellationToken cts)
        {
            //var fw = new FileSystemWatcher(path);
            //fw.Create

            while (!cts.IsCancellationRequested)
            {
                int count = 0;
                foreach (var file in Directory.EnumerateFiles(_root))
                {
                    var content = File.ReadAllText(file);
                    TMessage msg = JsonConvert.DeserializeObject<TMessage>(content);
                    await worker(msg);
                    File.Delete(file);
                    count++;
                }

                if (count == 0)
                {
                    break; // Done. 
                }

                await Task.Delay(TimeSpan.FromSeconds(1));

                // await Task.Delay(TimeSpan.FromSeconds(30));
            }
        }
    }

}
