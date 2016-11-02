using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace AzureStorageHelpers
{
    static class TableUtility
    {
        // See examples here: http://stackoverflow.com/questions/19972443/azure-table-storage-xml-serialization-for-tablecontinuationtoken 
        public static string SerializeToken(TableContinuationToken token)
        {
            if (token == null)
            {
                return null;
            }

            // Much more compact than XML serialization. 
            // XML serializing can be 300 bytes; then Base64 encoding can expand another 25%. 
            // Direct up serialization here can be 40bytes. 
            string s = string.Format("t,{0},{1},{2},{3},x",
                token.NextPartitionKey,
                token.NextRowKey,
                token.NextTableName,
                token.TargetLocation.HasValue ? (
                (token.TargetLocation.Value == StorageLocation.Primary) ? "P" : token.TargetLocation.Value.ToString()) :
                "");
            return s;        
        }

        private static string Norm(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
            {
                return null;
            }
            return s;
        }

        public static TableContinuationToken DeserializeToken(string token)
        {
            if (string.IsNullOrWhiteSpace(token))
            {
                return null;
            }
            try
            {
                var parts = token.Split(',');
                if (parts.Length != 6)
                {
                    throw new InvalidOperationException(); 
                }
                var header = parts[0];
                if (header != "t")
                {
                    throw new InvalidOperationException();
                }
                var footer = parts[5];
                if (footer != "x")
                {
                    throw new InvalidOperationException();
                }

                TableContinuationToken t = new TableContinuationToken();
                t.NextPartitionKey = Norm(parts[1]);
                t.NextRowKey = Norm(parts[2]);
                t.NextTableName = Norm(parts[3]);

                var loc = Norm(parts[4]);
                if (loc != null)
                {
                    if (loc == "P")
                    {
                        t.TargetLocation = StorageLocation.Primary;
                    }
                    else
                    {
                        t.TargetLocation = (StorageLocation) Enum.Parse(typeof(StorageLocation), loc);
                    }
                }

                return t;          
            }
            catch
            {
                throw new UserException(HttpStatusCode.BadRequest, "Continuation token is invalid");
            }
        }

        public static string EncodeBase64(string str)
        {
            byte[] bytes = Encoding.UTF8.GetBytes(str);
            string base64 = Convert.ToBase64String(bytes);
            return base64;
        }

        public static string DecodeBase64(string base64)
        {
            byte[] bytes = Convert.FromBase64String(base64);
            string str = Encoding.UTF8.GetString(bytes);
            return str;
        }
    }
}