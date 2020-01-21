using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Net;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // https://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-tables/#retrieve-a-range-of-entities-in-a-partition
    public class AzureTableStorage<T> : ITableStorage<T> where T : TableEntity, new()
    {
        private readonly CloudTable _table;

        public AzureTableStorage(string accountConnectionString, string tableName)
            : this(CloudStorageAccount.Parse(accountConnectionString), tableName)
        {
        }

        public AzureTableStorage(CloudStorageAccount account, string tableName)
        {
            CloudTableClient tableClient = account.CreateCloudTableClient();
            _table = tableClient.GetTableReference(tableName);
            _table.CreateIfNotExists();
        }

        // All must have the same partition key 
        public async Task WriteBatchAsync(T[] entities, TableInsertMode mode = TableInsertMode.Insert)
        {
            if (entities.Length == 0)
            {
                return; // nothing to write. 
            }
            string partitionKey = entities[0].PartitionKey;

            const int BatchSize = 99;

            TableBatchOperation batchOperation = new TableBatchOperation();
            foreach (var entity in entities)
            {
                if (entity.PartitionKey != partitionKey)
                {
                    throw new InvalidOperationException("All entities in a batch must have same partition key");
                }

                ValidateRowKey(entity.RowKey);

                switch (mode)
                {
                    case TableInsertMode.Insert:
                        batchOperation.Insert(entity);
                        break;
                    case TableInsertMode.InsertOrMerge:
                        batchOperation.InsertOrMerge(entity);
                        break;
                    case TableInsertMode.InsertOrReplace:
                        batchOperation.InsertOrReplace(entity);
                        break;
                    default:
                        throw new InvalidOperationException("Unsupported insert mode: " + mode.ToString());
                }

                if (batchOperation.Count == BatchSize)
                {
                    // Flush
                    await _table.ExecuteBatchAsync(batchOperation);
                    batchOperation = new TableBatchOperation();
                }
            }

            if (batchOperation.Count > 0)
            {
                await _table.ExecuteBatchAsync(batchOperation);
            }
        }

        public async Task WriteOneAsync(T entity, TableInsertMode mode = TableInsertMode.Insert)
        {
            // Create the TableOperation that inserts the customer entity.
            TableOperation op;

            switch (mode)
            {
                case TableInsertMode.Insert:
                    op = TableOperation.InsertOrReplace(entity);
                    break;
                case TableInsertMode.InsertOrMerge:
                    op = TableOperation.InsertOrMerge(entity);
                    break;

                case TableInsertMode.InsertOrReplace:
                    op = TableOperation.InsertOrReplace(entity);
                    break;

                default:
                    throw new InvalidOperationException("Unsupported insert mode: " + mode.ToString());
            }

            // Execute the insert operation.
            await _table.ExecuteAsync(op);
        }

        public async Task WriteOneMergeAsync(T entity)
        {
            // Create the TableOperation that inserts the customer entity.
            TableOperation insertOperation = TableOperation.InsertOrMerge(entity);

            // Execute the insert operation.
            await _table.ExecuteAsync(insertOperation);
        }

        public async Task<T> LookupOneAsync(string partitionKey, string rowKey)
        {
            TableOperation retrieveOperation = TableOperation.Retrieve<T>(partitionKey, rowKey);

            // Execute the retrieve operation.
            TableResult retrievedResult = await _table.ExecuteAsync(retrieveOperation);

            if (retrievedResult.Result != null)
            {
                T entity = (T)retrievedResult.Result;
                return entity;
            }

            // Not found 
            return null;
        }

        
        public async Task<Segment<T>> QueryAsync(
            TableQuery<T> query,
            string continuationToken = null)
        {
            TableContinuationToken realToken = TableUtility.DeserializeToken(continuationToken);
            var segment = await _table.ExecuteQuerySegmentedAsync(query, realToken);

            return new Segment<T>(segment.Results.ToArray(), TableUtility.SerializeToken(segment.ContinuationToken));
        }


        // http://stackoverflow.com/questions/18376087/copy-all-rows-to-another-table-in-azure-table-storage
        public async Task<Segment<T>> LookupAsync(
            string partitionKey,
            string rowKeyStart,
            string rowKeyEnd,
            string continuationToken)
        {
            TableQuery<T> query;
            if (partitionKey == null)
            {
                query = new TableQuery<T>();
            }
            else
            {
                string filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);

                if (rowKeyStart != null)
                {
                    filter = TableQuery.CombineFilters(filter, TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, rowKeyStart));
                }
                if (rowKeyEnd != null)
                {
                    filter = TableQuery.CombineFilters(filter, TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, rowKeyEnd));
                }

                query = new TableQuery<T>().Where(filter);
            }

            return await QueryAsync(query, continuationToken);
        }



        public async Task DeleteOneAsync(T entity)
        {
            // Passed security checks, do the actual delete. 
            TableOperation deleteOperation = TableOperation.Delete(entity);
            try
            {
                await _table.ExecuteAsync(deleteOperation);
            }
            catch (StorageException se)
            {
                if (se.RequestInformation.HttpStatusCode == 404)
                {
                    return;
                }
            }
        }

        // Very useful for tracking down errors in a batch. 
        public static void ValidateRowKey(string rowKey)
        {
            bool isValid = IsValidateRowKey(rowKey);
            if (!isValid)
            {
                throw new InvalidOperationException("Row key '" + rowKey + "' has invalid chars.");
            }
        }

        // https://blogs.msdn.microsoft.com/jmstall/2014/06/12/azure-storage-naming-rules/
        private static bool IsValidateRowKey(string rowKey)
        {
            if (string.IsNullOrWhiteSpace(rowKey))
            {
                return false;
            }
            foreach (var ch in rowKey)
            {
                var c = (short)ch;
                if (c < 0x1f)
                {
                    return false;
                }
                else if (c >= 0x7f && c <= 0x9f)
                {
                    return false;
                }
                else if (ch == '/' || ch == '\\' || ch == '#' || ch == '?')
                {
                    return false;
                }
            }
            return true;
        }

        // Etag documentation: https://azure.microsoft.com/en-us/blog/managing-concurrency-in-microsoft-azure-storage-2/
        public async Task<T> WriteAtomicAsync(string partionKey, string rowKey, 
            Func<T, Task> mutate,
            Func<Task<T>> create)
        {

            Retry:
            T entity = await this.LookupOneAsync(partionKey, rowKey);
            TableOperation insertOperation;
            if (entity == null)
            {
                // Doesn't exist yet.  
                entity = await create();

                // This will 409 if the entity already exists. 
                insertOperation = TableOperation.Insert(entity);
            }
            else
            {
                await mutate(entity);

                // This will 412 if the entity's etag doesn't match. 
                insertOperation = TableOperation.Merge(entity);
            }

            if (entity.PartitionKey != partionKey)
            {
                throw new InvalidOperationException("Illegal change of partition key");
            }
            if (entity.RowKey != rowKey)
            {
                throw new InvalidOperationException("Illegal change of row key");
            }

            try
            {
                // Insert/InsertReplace will full overwrite. 
                // Merge operation will set the Etag match header. 
                // Requires an etag, meaning we must have read it already. 
                // TableOperation insertOperation = TableOperation.Merge(entity);

                // Execute the insert operation.
                await _table.ExecuteAsync(insertOperation);

                return entity;

            }
            catch (StorageException ex)
            {
                // Check for retry

                // On Insert, already exists. 
                if (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    goto Retry;
                }

                // On merge, etag mismatch
                if (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
                {
                    // Etag mismatch. Retry! 
                    goto Retry;
                }
                throw;
            }
        }
    }
}