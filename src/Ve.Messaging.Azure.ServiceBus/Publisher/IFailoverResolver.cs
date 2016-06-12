namespace Ve.Messaging.Azure.ServiceBus.Publisher
{
    public interface IFailoverResolver
    {
        bool IsInFailover { get; }
        void ReportException();
        void ExitFailover();
    }
}