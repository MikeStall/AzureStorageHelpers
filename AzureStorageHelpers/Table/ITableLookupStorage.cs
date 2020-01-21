using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

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

        // Full query ability 
        Task<Segment<T>> QueryAsync(TableQuery<T> query, string continuationToken = null);
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

        /// <summary>
        /// Atomic create/update to a single table entity. 
        /// This will do retries and etag checks to ensure an atomic operation, so the mutate and create functions can be called multiple times. 
        /// Result of callbacks must preserve the partition and row key. 
        /// </summary>
        /// <param name="partionKey">parition key of entity</param>
        /// <param name="rowKey">row key of entity </param>
        /// <param name="mutate">If entity already exists, called to mutate it. </param>
        /// <param name="create">If entity does not yet exist, called to create it.</param>
        /// <returns>the resulting entity</returns>
        Task<T> WriteAtomicAsync(string partionKey, string rowKey, 
            Func<T, Task> mutate, // Called if already exists 
            Func<Task<T>> create); // Called if doesn't exist. 
    }

    public enum TableInsertMode
    {
        // These aren't concurrent safe. 
        Insert,
        InsertOrMerge,
        InsertOrReplace,
    }


    public static class ITableLookupStorageExtensions
    {
        public static async Task<T[]> QueryAllAsync<T>(
            this ITableLookupStorage<T> table,
               TableQuery<T> query) where T : TableEntity
        {
            int? takeN = query.TakeCount;
            List<T> list = null;
            string continuationToken = null;

            while (true)
            {
                var segment = await table.QueryAsync(query, continuationToken);
                if (list == null)
                {
                    if (segment.ContinuationToken == null)
                    {
                        if (takeN.HasValue)
                        {
                            // Truncate
                            if (segment.Results.Length > takeN.Value)
                            {
                                return segment.Results.Take(takeN.Value).ToArray();
                            }
                        }

                        return segment.Results; // optimization, skip allocating the list 
                    }
                    list = new List<T>();
                }
                if (segment.Results != null)
                {
                    list.AddRange(segment.Results);
                }

                if (takeN.HasValue)
                {
                    if (list.Count > takeN.Value)
                    {
                        // Truncate
                        list.RemoveRange(takeN.Value, list.Count - takeN.Value);
                    }

                    // If we continue querying, we'll grab more than TakeN. 
                    // So check and return now. 
                    if (list.Count == takeN.Value)
                    {
                        return list.ToArray();
                    }
                }

                continuationToken = segment.ContinuationToken;
                if (continuationToken == null)
                {
                    // Done
                    return list.ToArray();
                }
            }
        }

        public static async Task<T[]> LookupAsync<T>(this ITableLookupStorage<T> table,
            string partitionKey, string rowKeyStart = null, string rowKeyEnd = null) where T : TableEntity
        {
            TableQuery<T> query = new TableQuery<T>();
            if (partitionKey != null)
            {
                query.WhereRowRange(partitionKey, rowKeyStart, rowKeyEnd);
            }

            return await table.QueryAllAsync(query);             
        }

        #region Query Helpers 
        // Merge a filter into an existing filter
        public static TableQuery<T> AppendWhere<T>(
         this TableQuery<T> query,
         string filterString
         ) where T : TableEntity
        {
            if (query.FilterString != null)
            {
                filterString = TableQuery.CombineFilters(
                    query.FilterString,
                    TableOperators.And,
                    filterString);
            }

            query.Where(filterString); // fluent
            return query;
        }

        
        
        public static TableQuery<T> WhereEquals<T>(
            this TableQuery<T> query,
            string propertyName,
            object value
            ) where T : TableEntity
        {
            var prop = typeof(T).GetProperty(propertyName);
            if (prop == null)
            {
                throw new ArgumentException($"No property '{propertyName}'  on type '{typeof(T).Name}'");
            }

            string filter;
            if (prop.PropertyType == typeof(string))
            {
                filter = TableQuery.GenerateFilterCondition(
                    propertyName, QueryComparisons.Equal, (string)value);             
            } else if (prop.PropertyType == typeof(int))
            {
                filter = TableQuery.GenerateFilterConditionForInt(
                    propertyName, QueryComparisons.Equal, (int)value);
            } else if (prop.PropertyType == typeof(bool))
            {
                filter = TableQuery.GenerateFilterConditionForBool(
                    propertyName, QueryComparisons.Equal, (bool)value);
            } else
            {
                throw new ArgumentException($"Unsupported type '{prop.PropertyType.Name}' for property '{propertyName}'");
            }

            return query.AppendWhere(filter);
        }

        // If RowKeyStart/RowKeyEnd are null, then it's an open ended query. 
        public static TableQuery<T> WhereRowRange<T>(
            this TableQuery<T> query,
            string partitionKey,
            string rowKeyStart = null,
            string rowKeyEnd = null
        ) where T : TableEntity
        {
            if (partitionKey == null)
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

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

            return query.AppendWhere(filter);
        }

        public static TableQuery<T> WhereRowsWithPrefix<T>(
            this TableQuery<T> query,
            string partitionKey,
            string rowKeyPrefix) where T : TableEntity
        {
            string rowKeyStart = rowKeyPrefix;
            string rowKeyEnd = NextRowKey(rowKeyPrefix);
            return query.WhereRowRange(partitionKey, rowKeyStart, rowKeyEnd);
        }
        #endregion Query Helpers 

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