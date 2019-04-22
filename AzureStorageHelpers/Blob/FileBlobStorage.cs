using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
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
            await Task.Yield();

            string x = GetPath(path);
            if (!File.Exists(x))
            {
                return null;
            }
            for (int retry = 0; retry < 50; retry++)
            {
                try
                {
                    string contents = File.ReadAllText(x);
                    return contents;
                }
                catch
                {
                    Thread.Sleep(1);
                }
            }
            throw new InvalidOperationException("Retries failed");
        }

        public async Task WriteNewAsync(string path, string contents, IDictionary<string, string> metadataProperties = null)
        {
            await Task.Yield();

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
            await Task.Yield();

            string x = GetPath(path);
            if (!File.Exists(x))
            {
                return null;
            }

            for (int retry = 0; retry < 50; retry++)
            {
                // https://stackoverflow.com/questions/13698380/filestream-locking-a-file-for-reading-and-writing
                // $$$ Apply etag and optimistic concurrency 
                FileStream fs;
                try
                {
                    fs = new FileStream(x, FileMode.Open, FileAccess.ReadWrite);
                }
                catch(IOException e)
                {
                    Thread.Sleep(1);
                    continue; // retry
                }
                
                StreamReader sr = null;
                StreamWriter sw = null;

                try
                {
                    sr = new StreamReader(fs);
                    var contents = sr.ReadToEnd();

                    var tuple = await mutator(contents);
                    string newContent = tuple.Item1;

                    fs.Seek(0, SeekOrigin.Begin);
                    sw = new StreamWriter(fs);
                    sw.Write(newContent);
                    sw.Flush();
                    fs.SetLength(fs.Position);

                    return tuple;
                }
                finally
                {
                    if (sw != null)
                    {
                        sw.Close();
                    }
                    if (sr != null)
                    {
                        sr.Close();
                    }

                    fs.Dispose();
                }                
            }
            throw new InvalidOperationException("Retries failed");
        }

        // $$$ assumpe pathPrefix is a directory 
        public async Task<string[]> ListPathsAsync(string pathPrefix)
        {
            await Task.Yield();

            string filePath = GetPath(pathPrefix);
            List<string> l = new List<string>();
            Directory.CreateDirectory(filePath);

            ListPathsWorker(filePath, l);

            return l.ToArray();
        }
        private void ListPathsWorker(string filePath, List<string> files)
        {
            foreach (var file in Directory.EnumerateFiles(filePath))
            {
                files.Add(file.Replace('\\', '/')); // convert back to blob 
            }

            // Subdirs
            foreach(var dir in Directory.EnumerateDirectories(filePath))
            {
                ListPathsWorker(dir, files);
            }
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
            await Task.Yield();

            string x = GetPath(path);
            File.Delete(x);
        }

        public async Task WriteStompAsync(string path, string contents, IDictionary<string, string> metadataProperties = null)
        {
            await Task.Yield();

            string x = GetPath(path);
            Directory.CreateDirectory(Path.GetDirectoryName(x));
            File.WriteAllText(x, contents);
        }


        public async Task<Stream> OpenReadAsync(string path)
        {
            await Task.Yield();

            string x = GetPath(path);
            Stream fs = new FileStream(x, FileMode.Open, FileAccess.Read);
            return fs;
        }

        public async Task<Stream> OpenWriteStompAsync(string path)
        {

            await Task.Yield();
            string x = GetPath(path);
            Stream fs = new FileStream(x, FileMode.Create, FileAccess.Write);
            return fs;
        }
    }
}