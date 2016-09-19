using System;
using System.Net;

namespace AzureStorageHelpers
{
    // Like InvalidOperation, but includes an HttpStatusCode.
    // The status code would be (409, 404, 400) the error to return to the user assuming this 
    // call was made directly from a REST controller. 
    // Callers should have a handler to convert this exception to a code. 
    public class UserException : InvalidOperationException
    {
        public HttpStatusCode StatusCode { get; set; }

        public UserException(HttpStatusCode code, string message)
            : base(message)
        {
            StatusCode = code;
        }
    }
}