using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using System.Text;
using System.Globalization;
using System.Threading;

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

        // $$$ - Respect insert mode
        public async Task WriteBatchAsync(T[] entities, TableInsertMode mode)
        {
            await Task.Yield();
            lock (_lock)
            {
                foreach (var entity in entities)
                {
                    WriteOneWorker(entity);
                }
            }
        }

        public async Task WriteOneAsync(T entity, TableInsertMode mode )
        {
            await Task.Yield();
            lock (_lock)
            {
                WriteOneWorker(entity);
            }
        }

        // https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Understanding-the-Table-Service-Data-Model?redirectedfrom=MSDN
        // Azure table doesn't allow / \ # ? 
        // Files don't allow    /, \, ?, :, *, <,  >, | 
        // so # is a valid file escapacing characters 
        static string Escape(string key)
        {
            StringBuilder escapedStorageKey = new StringBuilder(key.Length);
            foreach (char c in key)
            {
                if (!char.IsLetterOrDigit(c))
                {
                    escapedStorageKey.Append(EscapeStorageCharacter(c));
                }
                else
                {
                    escapedStorageKey.Append(c);
                }
            }

            return escapedStorageKey.ToString();
        }

        private static string EscapeStorageCharacter(char character)
        {
            var ordinalValue = (ushort)character;
            if (ordinalValue < 0x100)
            {
                return string.Format(CultureInfo.InvariantCulture, "#{0:X2}", ordinalValue);
            }
            else
            {
                return string.Format(CultureInfo.InvariantCulture, "##{0:X4}", ordinalValue);
            }
        }

        // May throw on contention 
        private void WriteOneWorker(T entity)
        {
            string path = Path.Combine(_root, Escape(entity.PartitionKey), Escape(entity.RowKey)) + ".json";
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            File.WriteAllText(path, JsonConvert.SerializeObject(entity));
        }

        public async Task WriteOneMergeAsync(T entity)
        {
            await Task.Yield();

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
            await Task.Yield();

            if (entity.ETag == null)
            {
                throw new InvalidOperationException("Delete requires an Etag. Can use '*'.");
            }
            string path = Path.Combine(_root, Escape(entity.PartitionKey), Escape(entity.RowKey)) + ".json";

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
            await Task.Yield();

            return LookupOneWorker(partitionKey, rowKey);
        }

        private T LookupOneWorker(string partitionKey, string rowKey)
        {
            string path = Path.Combine(_root, Escape(partitionKey), Escape(rowKey)) + ".json";
            if (File.Exists(path))
            {
                return ReadEntity(path);
            }
            return null;
        }

        private static T ReadEntity(string path)
        {
            for (int retry = 0; retry < 50; retry++)
            {
                try
                {

                    string json = File.ReadAllText(path);
                    var obj = JsonConvert.DeserializeObject<T>(json);
                    obj.Timestamp = new FileInfo(path).LastWriteTimeUtc;
                    obj.ETag = new FileInfo(path).LastWriteTimeUtc.ToString();
                    return obj;
                }
                catch (IOException e)
                {
                    // Retry
                    Thread.Sleep(1);
                }
            }
            throw new InvalidOperationException("Exceeded retries on read");
        }

        public async Task<Segment<T>> LookupAsync(
            string partitionKey, 
            string rowKeyStart, 
            string rowKeyEnd,
            string continuationToken)
        {
            await Task.Yield();

            string[] paths;
            if (partitionKey == null)
            {
                // Return all
                paths = Directory.EnumerateDirectories(_root).ToArray();
            }
            else
            {
                string path = Path.Combine(_root, Escape(partitionKey));
                Directory.CreateDirectory(path);
                paths = new string[] { path };
            }

            List<T> l = new List<T>();
            foreach (var path in paths)
            {
                foreach (var file in Directory.EnumerateFiles(path))
                {
                    // Don't use rowKey from filename since that's escaped.
                    var entity = ReadEntity(file);
                    string rowKey = entity.RowKey;

                    // Always use .CompareOrdinal to get char-by-char comparison. 
                    if (rowKeyStart != null)
                    {
                        if (string.CompareOrdinal(rowKey, rowKeyStart) < 0)
                        {
                            continue;
                        }
                    }
                    if (rowKeyEnd != null)
                    {
                        if (string.CompareOrdinal(rowKey, rowKeyEnd) > 0)
                        {
                            continue;
                        }
                    }

                    l.Add(entity);
                }
            }

            // Excercise continuation tokens.  
            if (continuationToken == null)
            {
                if (l.Count == 0)
                {
                    return new Segment<T>(new T[0], null);
                }
                return new Segment<T>(new T[1] { l[0] }, "x");
            }
            else if (continuationToken == "x")
            {
                return new Segment<T>(l.Skip(1).ToArray());
            }
            else
            {
                throw new InvalidOperationException("illegal continuation token");
            }
        }

        public async Task<T> WriteAtomicAsync(string partionKey, string rowKey, 
            Func<T, Task> mutate,
            Func<Task<T>> create)
        {
            T entity = await this.LookupOneAsync(partionKey, rowKey);
            if (entity == null)
            {
                var newEntity = await create();
                await this.WriteOneAsync(newEntity, TableInsertMode.Insert);
                return newEntity;
            }
            await mutate(entity);
            await this.WriteOneAsync(entity, TableInsertMode.InsertOrMerge);
            return entity;
        }

        public Task<Segment<T>> QueryAsync(TableQuery<T> query, 
            string continuationToken = null)
        {
            throw new NotImplementedException();
        }
    }
}