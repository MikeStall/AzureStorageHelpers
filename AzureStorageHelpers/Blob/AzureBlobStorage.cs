using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{ 
    public class AzureBlobStorage : IBlobStorage2
    {
        private readonly CloudBlobContainer _container;

        public AzureBlobStorage(CloudStorageAccount account, string container)
        {
            CloudBlobClient client = account.CreateCloudBlobClient();
            _container = client.GetContainerReference(container);
            _container.CreateIfNotExists();
        }

        public AzureBlobStorage(string accountConnectionString, string container)
            : this(CloudStorageAccount.Parse(accountConnectionString), container)
        {
        }

        public async Task<string> ReadAsync(string path)
        {
            var blob = _container.GetBlockBlobReference(path);
            if (!blob.Exists())
            {
                return null;
            }
            string content = blob.DownloadText();
            return content;
        }

        public async Task WriteStompAsync(string path, string contents, IDictionary<string, string> metadataProperties = null)
        {
            await WriteInternalAsync(path, contents, true, metadataProperties);
        }

        public async Task WriteNewAsync(string path, string contents, IDictionary<string, string> metadataProperties = null)
        {
            await WriteInternalAsync(path, contents, false, metadataProperties);
        }

        private async Task WriteInternalAsync(string path, string contents, bool canOverwrite, IDictionary<string, string> metadataProperties = null)
        {            
            var blob = _container.GetBlockBlobReference(path);
            if (!canOverwrite && blob.Exists())
            {
                throw new UserException(HttpStatusCode.Conflict, "Sheet already exists.");
            }
            blob.UploadText(contents);

            if (metadataProperties != null)
            {
                foreach (var kv in metadataProperties)
                {
                    if (!string.IsNullOrWhiteSpace(kv.Value))
                    {
                        blob.Metadata[kv.Key] = kv.Value;
                    }
                }
                await blob.SetMetadataAsync();
            }
        }
             

        public async Task<Tuple<string, string>> MutateAsync(string path, Func<string, Task<Tuple<string, string>>> mutator)
        {
            var blob = _container.GetBlockBlobReference(path);
            if (!blob.Exists())
            {
                return null;
            }
            
            // See for concurrency: http://azure.microsoft.com/blog/2014/09/08/managing-concurrency-in-microsoft-azure-storage-2/ 
        Retry:
            // Apply etag and optimistic concurrency             
            string contents = blob.DownloadText();
            string etag = blob.Properties.ETag;

            var tuple = await mutator(contents);
            string content = tuple.Item1;

            try
            {
                blob.UploadText(content, accessCondition: AccessCondition.GenerateIfMatchCondition(etag));
            }
            catch (StorageException ex)
            {
                if (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    // Etag mismatch. Retry! 
                    goto Retry;
                }
                throw;
            }

            return tuple;
        }

        public async Task<string[]> ListSubDirsAsync(string pathPrefix)
        {
            BlobContinuationToken continuationToken = null;

            List<string> list = new List<string>();

            do
            {
                const int BatchSize = 100;
                bool useFlatBlobListing = false; // only subdirs
                BlobListingDetails details = BlobListingDetails.None; // only  committed blobs, no metadata
                BlobResultSegment segment = await _container.ListBlobsSegmentedAsync(
                    pathPrefix, useFlatBlobListing, details, BatchSize, continuationToken, null, null);
                continuationToken = segment.ContinuationToken;

                foreach (IListBlobItem item in segment.Results)
                {
                    var dir = item as CloudBlobDirectory;
                    if (dir == null)
                    {
                        continue;
                    }

                    list.Add(dir.Prefix);
                }
            } while (continuationToken != null);

            return list.ToArray();
        }

        public async Task<string[]> ListPathsAsync(string pathPrefix)
        {
            BlobContinuationToken continuationToken = null;
                        
            List<string> list = new List<string>();

            do
            {
                const int BatchSize = 100;
                bool useFlatBlobListing = true; // files, full recursive, 
                BlobListingDetails details = BlobListingDetails.None; // only  committed blobs, no metadata
                BlobResultSegment segment = await _container.ListBlobsSegmentedAsync(
                    pathPrefix, useFlatBlobListing, details, BatchSize, continuationToken, null, null);
                continuationToken = segment.ContinuationToken;

                foreach (IListBlobItem item in segment.Results)
                {
                    var blob = item as CloudBlockBlob;
                    if (blob == null)
                    {
                        continue;
                    }

                    list.Add(blob.Name);
                }
            } while (continuationToken != null);

            return list.ToArray();
        }


        public async Task<BlobInfo> GetPropertiesAsync(string path)
        {
            var blob = _container.GetBlockBlobReference(path);

            try
            {
                await blob.FetchAttributesAsync();

                return new BlobInfo
                {
                    Name = blob.Name,
                    Size = blob.Properties.Length,
                    Timestamp = blob.Properties.LastModified.Value.DateTime,
                    Properties = blob.Metadata
                };
            }
            catch (StorageException e)
            {
                // Not found. 
                return null;
            }
        }

        public async Task DeleteAsync(string path)
        {
            // Backup somewhere safe just so we can restore if there's an accident.             
            {
                string savePath = "save/" + path;
                string value = await this.ReadAsync(path);
                if (value != null)
                {
                    var saveBlob = _container.GetBlockBlobReference(path);
                    await saveBlob.UploadTextAsync(value);
                }
            }
            
            var blob = _container.GetBlockBlobReference(path);
            await blob.DeleteIfExistsAsync();
        }


        public async Task<Stream> OpenReadAsync(string path)
        {
            var blob = _container.GetBlockBlobReference(path);
            if (!blob.Exists())
            {
                return null;
            }
            Stream stream = await blob.OpenReadAsync();
            return stream;
        }
    }
}