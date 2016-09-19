using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // Simulate storage on the file system
    // For local testing. 
    public class FileTableStorage<T> : ITableStorage<T> where T : TableEntity, new()
    {
        private readonly string _root;

        public FileTableStorage(string root, string tableName)
        {
            _root = Path.Combine(root, "table", tableName);
            Directory.CreateDirectory(_root);
        }

        // Lock for dealingw with contention writes. 
        // Not bullet proof (doesn't work cross-process); but good enough for local testing. 
        object _lock = new object();

        public async Task WriteBatchAsync(T[] entities)
        {
            lock (_lock)
            {
                foreach (var entity in entities)
                {
                    WriteOneWorker(entity);
                }
            }
        }

        public async Task WriteOneAsync(T entity)
        {
            lock (_lock)
            {
                WriteOneWorker(entity);
            }
        }

        // May throw on contention 
        private void WriteOneWorker(T entity)
        {
            string path = Path.Combine(_root, entity.PartitionKey, entity.RowKey) + ".json";
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            File.WriteAllText(path, JsonConvert.SerializeObject(entity));
        }

        public async Task WriteOneMergeAsync(T entity)
        {
            lock (_lock)
            {
                var existing = LookupOneWorker(entity.PartitionKey, entity.RowKey);
                if (existing == null)
                {
                    WriteOneWorker(entity);
                    return; // success
                }

                // Merge in. 
                foreach (var prop in typeof(T).GetProperties(BindingFlags.Instance | BindingFlags.Public))
                {
                    var val = prop.GetValue(entity);
                    if (val != null)
                    {
                        prop.SetValue(existing, val);
                    }
                }
                WriteOneWorker(existing);
                return; // success
            }                
        }

        public async Task DeleteOneAsync(T entity)
        {
            if (entity.ETag == null)
            {
                throw new InvalidOperationException("Delete requires an Etag. Can use '*'.");
            }
            string path = Path.Combine(_root, entity.PartitionKey, entity.RowKey) + ".json";

            string currentEtag = new FileInfo(path).LastWriteTimeUtc.ToString();
            if (entity.ETag != "*" && entity.ETag != currentEtag)
            {
                throw new InvalidOperationException("Etag mismatch"); // Todo - give real error (429?)
            }

            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }

        public async Task<T> LookupOneAsync(string partitionKey, string rowKey)
        {
            return LookupOneWorker(partitionKey, rowKey);
        }

        private T LookupOneWorker(string partitionKey, string rowKey)
        {
            string path = Path.Combine(_root, partitionKey, rowKey) + ".json";
            if (File.Exists(path))
            {
                return ReadEntity(path);
            }
            return null;
        }

        private static T ReadEntity(string path)
        {
            string json = File.ReadAllText(path);
            var obj = JsonConvert.DeserializeObject<T>(json);
            obj.Timestamp = new FileInfo(path).LastWriteTimeUtc;
            obj.ETag = new FileInfo(path).LastWriteTimeUtc.ToString();
            return obj;
        }

        public async Task<T[]> LookupAsync(string partitionKey, string rowKeyStart = null, string rowKeyEnd = null)
        {
            string path = Path.Combine(_root, partitionKey);
            Directory.CreateDirectory(path);

            List<T> l = new List<T>();
            foreach (var file in Directory.EnumerateFiles(path))
            {
                string rowKey = Path.GetFileNameWithoutExtension(file);

                if (rowKeyStart != null)
                {
                    if (string.Compare(rowKey, rowKeyStart) < 0)
                    {
                        continue;
                    }
                }
                if (rowKeyEnd != null)
                {
                    if (string.Compare(rowKey, rowKeyEnd) > 0)
                    {
                        continue;
                    }
                }

                l.Add(ReadEntity(file));
            }
            return l.ToArray();
        }    
    }
}