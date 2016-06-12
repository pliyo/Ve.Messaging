using System.Threading;

namespace Ve.Messaging.Azure.ServiceBus.Publisher
{
    public class FailoverResolver : IFailoverResolver
    {
        private static bool _isInFailover;
        internal static int _errors;
        internal const int MAX_ERROR_NUMBER = 5;

        public bool IsInFailover
        {
            get { return _isInFailover; }
        }

        public void ReportException()
        {
            Interlocked.Increment(ref _errors);
            if (_errors > MAX_ERROR_NUMBER && !_isInFailover)
            {
                StartFailover();
            }
        }

        private void StartFailover()
        {
            _isInFailover = true;
        }

        public void ExitFailover()
        {
            if (_isInFailover)
            {
                _isInFailover = false;
                _errors = 0;
            }
        }
    }
}
