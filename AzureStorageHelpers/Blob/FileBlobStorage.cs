using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // Useful for local debugging
    public class FileBlobStorage : IBlobStorage, IBlobStorage2
    {
        private readonly string _root;

        public FileBlobStorage(string root, string container)
            : this(Path.Combine(root, "blob", container))
        {
        }

        public FileBlobStorage(string root)
        {
            _root = root;
            Directory.CreateDirectory(root);
        }

        string GetPath(string path)
        {
            var x = Path.Combine(_root, path.Replace('/', '\\'));
            return x;
        }

        public async Task<string> ReadAsync(string path)
        {
            string x = GetPath(path);
            if (!File.Exists(x))
            {
                return null;
            }
            string contents = File.ReadAllText(x);
            return contents;
        }

        public async Task WriteNewAsync(string path, string contents, IDictionary<string, string> metadataProperties = null)
        {
            string x = GetPath(path);
            if (File.Exists(x))
            {
                throw new UserException(HttpStatusCode.Conflict, "Sheet already exists.");
            }
            Directory.CreateDirectory(Path.GetDirectoryName(x));
            File.WriteAllText(x, contents);
        }

        public async Task<Tuple<string, string>> MutateAsync(string path, Func<string, Task<Tuple<string, string>>> mutator)
        {
            string x = GetPath(path);
            if (!File.Exists(x))
            {
                return null;
            }

            // $$$ Apply etag and optimistic concurrency 
            string contents = File.ReadAllText(x);

            var tuple = await mutator(contents);
            string newContent = tuple.Item1;

            File.WriteAllText(x, newContent);

            return tuple;
        }

        // $$$ assumpe pathPrefix is a directory 
        public async Task<string[]> ListPathsAsync(string pathPrefix)
        {
            string filePath = GetPath(pathPrefix);
            List<string> l = new List<string>();
            Directory.CreateDirectory(filePath);
            foreach (var file in Directory.EnumerateFiles(filePath))
            {
                l.Add(file.Replace('\\', '/')); // convert back to blob 
            }
            return l.ToArray();
        }

        public Task<string[]> ListSubDirsAsync(string pathPrefix)
        {
            throw new NotImplementedException();
        }

        public Task<BlobInfo> GetPropertiesAsync(string path)
        {
            throw new NotImplementedException();
        }

        public async Task DeleteAsync(string path)
        {
            string x = GetPath(path);
            File.Delete(x);
        }

        public async Task WriteStompAsync(string path, string contents, IDictionary<string, string> metadataProperties = null)
        {
            string x = GetPath(path);
            Directory.CreateDirectory(Path.GetDirectoryName(x));
            File.WriteAllText(x, contents);
        }


        public Task<Stream> OpenReadAsync(string path)
        {
            string x = GetPath(path);
            Stream fs = new FileStream(x, FileMode.Open, FileAccess.Read);
            return Task.FromResult(fs);
        }
    }
}