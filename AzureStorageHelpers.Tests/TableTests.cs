using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;
using System.IO;
using System.Collections.Generic;

namespace AzureStorageHelpers.Tests
{
    [TestClass]
    public class TableTests
    {
        static IStorageAccount GetStorage()
        {
            //return new StorageAccount.FileStorageAccount(@"c:\temp\92");
            return new StorageAccount.AzureStorageAccount(File.ReadAllText(@"c:\temp\dummy-storage-string.txt"));
        }

        [TestMethod]
        public async Task ContinuationTokens()
        {
            var storage = GetStorage();

            var table = storage.NewTable<MyEntity>("table2");

            // Must write 1000+ entries to force a continuation token 
            int basex = 3000;
            List<MyEntity> list = new List<MyEntity>();
            for (int i = 0; i < 1500; i++)
            {
                list.Add(new MyEntity { 
                    PartitionKey = "1", 
                    RowKey = i.ToString("d10"), 
                    Value = i + basex });
            }
            await table.WriteBatchAsync(list.ToArray(), TableInsertMode.InsertOrReplace);

            // Continuation tokens.
            // Only way to force a continuation token is to have 1000+ items. 

            var segment = await table.LookupAsync(null, null, null, null);
            Assert.AreEqual(basex, segment.Results[0].Value);

            for (int i = 0; i < segment.Results.Length; i++)
            {
                Assert.AreEqual(list[i], segment.Results[i]);
            }


            var cont1 = segment.ContinuationToken;

            var segment1 = await table.LookupAsync(null, null, null, cont1);
            var segment2 = await table.LookupAsync(null, null, null, cont1);

            Assert.AreEqual(segment1.ContinuationToken, segment2.ContinuationToken);
            Assert.AreEqual(segment1.Results.Length, segment2.Results.Length);
            for (int i = 0; i < segment1.Results.Length; i++)
            {
                Assert.AreEqual(segment1.Results[i], segment2.Results[i]);
            }

            Assert.AreEqual(segment.Results.Length + basex, segment1.Results[0].Value);
        }

        [TestMethod]
        public async Task TestEscapes()
        {
            var storage = GetStorage();

            var table = storage.NewTable<MyEntity>("tableEscape");

            // Pick wacky characters that are illegal in file systems 
            string pk = Guid.NewGuid().ToString();
            string rk = "ab:*<>|";
            await table.WriteOneAsync(new MyEntity { PartitionKey = pk, RowKey = rk, Value = 10 });

            var entities = await table.LookupAsync(pk);

            Assert.AreEqual(1, entities.Length);
            Assert.AreEqual(pk, entities[0].PartitionKey);
            Assert.AreEqual(rk, entities[0].RowKey);
            Assert.AreEqual(10, entities[0].Value);
        }


        [TestMethod]
        public async Task TestMethod1()
        {
            var storage = GetStorage();

            var table = storage.NewTable<MyEntity>("table1");

            await table.WriteOneAsync(new MyEntity { PartitionKey = "1", RowKey = "A1", Value = 10 });
            await table.WriteOneAsync(new MyEntity { PartitionKey = "1", RowKey = "A2", Value = 20 });
            await table.WriteOneAsync(new MyEntity { PartitionKey = "1", RowKey = "B2", Value = 30 });
            await table.WriteOneAsync(new MyEntity { PartitionKey = "2", RowKey = "B1", Value = 40 });

            var e2 = await table.LookupOneAsync("1", "A2");
            Assert.AreEqual(20, e2.Value);

            AssertRows(
                await table.LookupAsync("1", "A"),  // Prefix
                10, 20, 30);

            AssertRows(
                await table.LookupAsync("1", null), // Whole partition 
                10, 20, 30);

            AssertRows(
                await table.LookupAsync("1", "A2", "C"), // Range
                20, 30);

            AssertRows(
              await table.LookupAsync("1", "A2", "B2"), // Range, inclusive end
              20, 30);

            AssertRows(
              await table.LookupAsync("1", "A2", "A2"), // single, inclusive end,. 
              20);

            AssertRows(
                await table.GetRowsWithPrefixAsync("1", "A"),
                10, 20);

            AssertRows(
                await table.GetRowsWithPrefixAsync("1", "A2"),
                20);

            // Empty 
            AssertRows(
                await table.GetRowsWithPrefixAsync("1", "C"));

            AssertRows(
                await table.LookupAsync("3"));
                    
            // Delete 
            AssertRows(
                await table.LookupAsync(null), // Entire table
                10, 20, 30, 40);

            await table.DeleteOneAsync(e2);
            e2 = await table.LookupOneAsync("1", "A2");
            Assert.IsNull(e2);

        }

        static void AssertRows(MyEntity[] rows, params int[] values)
        {
            Assert.AreEqual(rows.Length, values.Length);
            for (int i = 0; i < values.Length; i++)
            {
                Assert.AreEqual(rows[i].Value, values[i]);
            }
        }


    }


    public class MyEntity : TableEntity
    {
        public int Value { get; set; }

        public override bool Equals(object obj)
        {
            var other = obj as MyEntity;
            if (other == null)
            {
                return false;
            }
            return (this.PartitionKey == other.PartitionKey) &&
                (this.RowKey == other.RowKey) &&
                (this.Value == other.Value);
        }
        public override int GetHashCode()
        {
            return this.Value;
        } 
    }
}
