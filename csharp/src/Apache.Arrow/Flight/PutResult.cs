using System;
using System.Collections.Generic;
using System.Text;
using Google.Protobuf;

namespace Apache.Arrow.Flight
{
    public class PutResult : IDisposable
    {
        private bool _disposed;

        public PutResult(ArrowBuffer metadata)
        {
            ApplicationMetadata = metadata;
        }

        public PutResult(Protocol.PutResult putResult)
        {
            this.ApplicationMetadata = new ArrowBuffer(putResult.AppMetadata.Memory);
        }

        public ArrowBuffer ApplicationMetadata { get; }

        public Protocol.PutResult ToProtocol()
        {
            return new Protocol.PutResult()
            {
                AppMetadata = ByteString.CopyFrom(ApplicationMetadata.Span)
            };
        }

        ~PutResult()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    ApplicationMetadata.Dispose();
                }
                _disposed = true;
            }
        }


        public void Dispose()
        {
            Dispose(true);
        }
    }
}
