using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.IO;

namespace AzureStorageHelpers
{
    // Abstraction over Azure storage. 
    public interface IStorageAccount
    {
        // Get a display name without secrets
        string GetDisplayName();

        IQueue<TMessage> NewQueue<TMessage>(string queueName);
        ITableStorage<TEntity> NewTable<TEntity>(string tableName) where TEntity : TableEntity, new();
        IBlobStorage2 NewBlobStorage(string container);
    }


    public class StorageAccount
    {
        public static IStorageAccount Parse(string acs)
        {
            if (Path.IsPathRooted(acs))
            {
                return new FileStorageAccount(acs); 
            }
            else
            {
                return new AzureStorageAccount(acs);
            }
        }

        public class AzureStorageAccount : IStorageAccount
        {
            private readonly string _acs;

            public AzureStorageAccount(string acs)
            {
                CloudStorageAccount.Parse(acs); // Will throw on errors
                _acs = acs;

            }

            public IQueue<TMessage> NewQueue<TMessage>(string queueName)
            {
                return new AzureQueue<TMessage>(_acs, queueName);
            }

            public ITableStorage<TEntity> NewTable<TEntity>(string tableName) where TEntity : TableEntity, new()
            {
                return new AzureTableStorage<TEntity>(_acs, tableName);
            }

            public IBlobStorage2 NewBlobStorage(string container)
            {
                return new AzureBlobStorage(_acs, container);
            }

            public string GetDisplayName()
            {
                CloudStorageAccount account;
                if (CloudStorageAccount.TryParse(_acs, out account))
                {
                    return account.Credentials.AccountName;
                }
                return "???";
            }
        }

        public class FileStorageAccount : IStorageAccount
        {
            private readonly string _path;
            public FileStorageAccount(string path)
            {
                _path = path;
            }
            public IQueue<TMessage> NewQueue<TMessage>(string queueName)
            {
                return new FileQueue<TMessage>(_path, queueName);
            }

            public ITableStorage<TEntity> NewTable<TEntity>(string tableName) where TEntity : TableEntity, new()
            {
                return new FileTableStorage<TEntity>(_path, tableName);
            }

            public IBlobStorage2 NewBlobStorage(string container)
            {
                return new FileBlobStorage(_path, container);
            }

            public string GetDisplayName()
            {
                return _path;
            }
        }
    }
}