using System;

namespace Tomatwo.DataStore
{
    public class DuplicateDocumentException : Exception
    {
        public DuplicateDocumentException()
        {
        }

        public DuplicateDocumentException(string message) : base(message)
        {
        }

        public DuplicateDocumentException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
