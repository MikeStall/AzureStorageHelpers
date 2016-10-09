using System;
using System.Net;

namespace AzureStorageHelpers
{
    public class Segment<T>
    {
        public string ContinuationToken { get; set; }
        public T[] Results { get; set; }

        public Segment() { }

        public Segment(T[] results)
        {
            this.Results = results;
        }
        public Segment(T[] results, string continuationToken)
        {
            this.Results = results;
            this.ContinuationToken = continuationToken;
        }
    }
}