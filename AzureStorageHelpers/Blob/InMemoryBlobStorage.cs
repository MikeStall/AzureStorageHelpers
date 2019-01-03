using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // In-memory abstraction for blob storage. 
    public class InMemoryStorage : IBlobStorage
    {
        Dictionary<string, string> _contents = new Dictionary<string, string>();
        public async Task<string> ReadAsync(string path)
        {
            string value;
            _contents.TryGetValue(path, out value);
            return value;
        }

        public async Task WriteNewAsync(string path, string contents, IDictionary<string, string> metadataProperties = null)
        {
            _contents.Add(path, contents);
        }

        public async Task<Tuple<string, string>> MutateAsync(string path, Func<string, Task<Tuple<string, string>>> mutator)
        {
            string contents = await this.ReadAsync(path);
            if (contents == null)
            {
                return null;
            }

            var tuple = await mutator(contents);
            string newValue = tuple.Item1;

            _contents[path] = newValue;

            return tuple;
        }


        public Task<string[]> ListPathsAsync(string pathPrefix)
        {
            throw new NotImplementedException();
        }

        public Task<string[]> ListSubDirsAsync(string pathPrefix)
        {
            throw new NotImplementedException();
        }


        public Task<BlobInfo> GetPropertiesAsync(string path)
        {
            throw new NotImplementedException();
        }


        public Task<System.IO.Stream> OpenReadAsync(string path)
        {
            throw new NotImplementedException();
        }

        public Task<Stream> OpenWriteStompAsync(string path)
        {
           throw new NotImplementedException();
        }
    }
}