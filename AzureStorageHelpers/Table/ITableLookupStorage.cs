using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
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
        Task<Segment<T>> LookupAsync(string partitionKey, string rowKeyStart, string rowKeyEnd, string continuationToken);
    }

    // Includes mutable operations. 
    public interface ITableStorage<T> : ITableLookupStorage<T> where T : TableEntity
    {
        // Write a single entity to table storage. 
        // InsertOrReplace semantics. 
        // Ok to overwrite. 
        Task WriteOneAsync(T entity, TableInsertMode mode = TableInsertMode.Insert);

        // Insert batch of entities. 
        // This can be > 100, but atomicity may only occur in chunks of 100. 
        // Must all share  the same partition key. 
        Task WriteBatchAsync(T[] entities, TableInsertMode mode = TableInsertMode.Insert);
        
        // Write with InsertOrMerge semantics. 
        Task WriteOneMergeAsync(T entity);
                
        Task DeleteOneAsync(T entity);
    }

    public enum TableInsertMode
    {
        Insert,
        InsertOrMerge,
        InsertOrReplace,
    }


    public static class ITableLookupStorageExtensions
    {
        public static async Task<T[]> LookupAsync<T>(this ITableLookupStorage<T> table,
            string partitionKey, string rowKeyStart = null, string rowKeyEnd = null) where T : TableEntity
        {
            List<T> list = null;
            string continuationToken = null;

            while (true)
            {
                var segment = await table.LookupAsync(partitionKey, rowKeyStart, rowKeyEnd, continuationToken);
                if (list == null)
                {
                    if (segment.ContinuationToken == null)
                    {
                        return segment.Results; // optimization, skip allocating the list 
                    }
                    list = new List<T>();
                }
                if (segment.Results != null)
                {
                    list.AddRange(segment.Results);
                }
                continuationToken = segment.ContinuationToken;
                if (continuationToken == null)
                {
                    // Done
                    return list.ToArray();
                }
            }
        }

        // Given a rowkey prefix, generate the next prefix. This can be used to find all row keys with a given prefix. 
        internal static string NextRowKey(string rowKeyStart)
        {
            int len = rowKeyStart.Length;
            char ch = rowKeyStart[len - 1];
            char ch2 = (char)(((int)ch) + 1);

            var x = rowKeyStart.Substring(0, len - 1) + ch2;
            return x;
        }

        public static Task<T[]> GetRowsWithPrefixAsync<T>(
            this ITableLookupStorage<T> table, 
            string partitionKey,
            string rowKeyPrefix) where T : TableEntity
        {
            string rowKeyEnd = NextRowKey(rowKeyPrefix);
            return table.LookupAsync(partitionKey, rowKeyPrefix, rowKeyEnd);
        }
    }
}