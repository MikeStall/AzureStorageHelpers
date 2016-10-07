using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading.Tasks;
using System.IO;

namespace AzureStorageHelpers.Tests
{
    [TestClass]
    public class TableTests
    {
        static IStorageAccount GetStorage()
        {
            return new StorageAccount.FileStorageAccount(@"c:\temp\92");
            //return new StorageAccount.AzureStorageAccount(File.ReadAllText(@"c:\temp\dummy-storage-string.txt"));
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
                20,30);

            AssertRows(
              await table.LookupAsync("1", "A2", "B2"), // Range, inclusive end
              20, 30);

            AssertRows(
                await table.GetRowsWithPrefixAsync("1", "A"),
                10, 20);

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
    }
}
