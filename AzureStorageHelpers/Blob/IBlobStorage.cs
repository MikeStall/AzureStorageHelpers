using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // Abstraction over a CloubBlob
    public class BlobInfo
    {
        public string Name { get; set; }
        public DateTime Timestamp { get; set; }
        public IDictionary<string, string> Properties { get; set; }
        public long Size { get; set; }
    };

    // Beware: blob paths are case sensitive. 
    // Naming rules:
    //   Container Name 3-63 lowercase alphanumeric and dash 
    //   Blob Name 1-1024 case-sensitive any url char 
    public interface IBlobStorage
    {
        // Read. 
        // Null if file does not yet exist. 
        Task<string> ReadAsync(string path);

        /// <summary>
        /// Open stream for reading this blob. Useful for incremental reads. 
        /// </summary>
        /// <param name="path">identifier</param>
        /// <returns>a stream. Null if blob doesn't exist.</returns>
        Task<Stream> OpenReadAsync(string path);

        // List all paths with the given prefix.         
        Task<string[]> ListPathsAsync(string pathPrefix);

        // List immediate subdirs. These are not valid paths, but prefixes of paths.
        // Will include a trailing / at the end.
        Task<string[]> ListSubDirsAsync(string pathPrefix);

        // Get blob properties
        // Return null if not found. 
        Task<BlobInfo> GetPropertiesAsync(string path);

        // Write. Throw  409  if the file already exists 
        // metadataProperties - optional. Will set as metadata
        Task WriteNewAsync(string path, string contents, IDictionary<string, string> metadataProperties = null);

        // Return null if path does not exist. 
        // Mutate is passed the incoming blob contents, and returns a tuple of (new contents, arbitrary tag from mutator)
        // Exceptions from mutator are passed up. 
        Task<Tuple<string, string>> MutateAsync(string path, Func<string, Task<Tuple<string, string>>> mutator);
    }

    // Move Delete onto a separate interface since it's extra-dangerous. 
    public interface IBlobStorage2 : IBlobStorage
    {
        Task DeleteAsync(string path);

        // Write, can stomp values (overwrite existing). 
        Task WriteStompAsync(string path, string contents, IDictionary<string, string> metadataProperties = null);
    }

    public static class IBlobStorageExtensions
    {
        public static async Task WriteNewJsonAsync<T>(this IBlobStorage storage, string path, T obj)
        {
            string json = JsonConvert.SerializeObject(obj, Formatting.Indented);
            await storage.WriteNewAsync(path, json);
        }

        public static async Task WriteStompAsync<T>(this IBlobStorage2 storage, string path, T obj)
        {
            string json = JsonConvert.SerializeObject(obj, Formatting.Indented);
            await storage.WriteStompAsync(path, json);
        }


        public static async Task<T> ReadJsonAsync<T>(this IBlobStorage storage, string path) where T : class
        {
            string json = await storage.ReadAsync(path);
            if (json == null)
            {
                return null;
            }
            T obj = JsonConvert.DeserializeObject<T>(json);
            return obj;
        }

        // Invoked a callback with the T. Mutate the T and write. 
        // Uses optimistic concurrency, returns the latest mutation 
        // Null if not found 
        public static async Task<T> MutateJsonAsync<T>(this IBlobStorage storage, string path, Action<T> mutator) where T : class
        {
            T obj = null;
            var tuple = await storage.MutateAsync(path, json =>
            {
                obj = JsonConvert.DeserializeObject<T>(json);

                // Not Async! 
                mutator(obj); // edits in place

                json = JsonConvert.SerializeObject(obj, Formatting.Indented);

                return Task.FromResult(Tuple.Create(json, (string)null));
            });

            return obj;
        }

        // Overload to catch async usage.
        // USe the synchronous version instead. 
        public static async Task<T> MutateJsonAsync<T>(this IBlobStorage storage, string path, Func<T, Task> mutator) where T : class
        {
            throw new NotImplementedException();
        }
    }

}