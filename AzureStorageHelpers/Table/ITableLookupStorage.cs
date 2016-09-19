using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;

namespace AzureStorageHelpers
{
    // Lookup-only operations on tables. For safety.
    // Naming rules:
    //  TableName 3-63 case-insensitive alphanumeric 
    //  Don't use \/#? in part/rowkey. 
    // https://msdn.microsoft.com/library/azure/dd179338.aspx
    public interface ITableLookupStorage<T> where T : TableEntity
    {
        // Null if not found. 
        Task<T> LookupOneAsync(string partitionKey, string rowKey);

        // Find all rows in this partition.
        // If PartitionKey = null, find all entities. 
        // Inclusive
        Task<T[]> LookupAsync(string partitionKey, string rowKeyStart = null, string rowKeyEnd = null);
    }

    // Includes mutable operations. 
    public interface ITableStorage<T> : ITableLookupStorage<T> where T : TableEntity
    {
        // Write a single entity to table storage. 
        // InsertOrReplace semantics. 
        // Ok to overwrite. 
        Task WriteOneAsync(T entity);

        // Insert batch of entities. 
        // This can be > 100, but atomicity may only occur in chunks of 100. 
        // Must all share  the same partition key. 
        Task WriteBatchAsync(T[] entities);

        // Write with InsertOrMerge semantics. 
        Task WriteOneMergeAsync(T entity);
                
        Task DeleteOneAsync(T entity);
    }
}