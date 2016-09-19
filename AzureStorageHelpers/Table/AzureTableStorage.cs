using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
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
        public async Task WriteBatchAsync(T[] entities)
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
                batchOperation.Insert(entity);
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

        public async Task WriteOneAsync(T entity)
        {
            // Create the TableOperation that inserts the customer entity.
            TableOperation insertOperation = TableOperation.InsertOrReplace(entity);

            // Execute the insert operation.
            await _table.ExecuteAsync(insertOperation);
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

        // http://stackoverflow.com/questions/18376087/copy-all-rows-to-another-table-in-azure-table-storage
        private T[] FetchAllEntities()
        {
            List<T> allEntities = new List<T>();
            TableContinuationToken tableContinuationToken = null;

            TableQuery<T> query = new TableQuery<T>();
            do
            {
                var queryResponse = _table.ExecuteQuerySegmented<T>(query, tableContinuationToken, null, null);
                tableContinuationToken = queryResponse.ContinuationToken;
                allEntities.AddRange(queryResponse.Results);
            }
            while (tableContinuationToken != null);
            return allEntities.ToArray();
        }

        public async Task<T[]> LookupAsync(string partitionKey, string rowKeyStart = null, string rowKeyEnd = null)
        {
            if (partitionKey == null)
            {
                // Get all rows
                return FetchAllEntities();               
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

                TableQuery<T> query = new TableQuery<T>().Where(filter);

                var rows = _table.ExecuteQuery(query); // This will return all rows (well over 1000)

                var array = rows.ToArray();
                return array;
            }
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
    }
}