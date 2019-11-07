using System;
using System.Threading;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace FunctionsWithLock
{
    public static class QueueProcessors
    {
        private static readonly object cacheLock = new object();
        private static DateTime? Timeout;

        [Singleton(Mode = SingletonMode.Listener)]
        [FunctionName("HighPriorityQueueMessageProcessor")]
        public static void Run([ServiceBusTrigger("highpriority", Connection = "ConnectionStrings:ServiceBus")]string myQueueItem, ILogger log)
        {
            lock (cacheLock)
            {
                Timeout = DateTime.Now.AddMinutes(1);

                Thread.Sleep(500);
                log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");

                Timeout = null;
            }
        }

        [Singleton(Mode = SingletonMode.Listener)]
        [FunctionName("LowPriorityQueueMessageProcessor")]
        public static void Run2([ServiceBusTrigger("lowpriority", Connection = "ConnectionStrings:ServiceBus")]string myQueueItem, ILogger log)
        {
            while (Timeout.HasValue)
            {
                var sleepTime = DateTime.Now - Timeout.Value;
                Thread.Sleep(sleepTime);
            }

            lock (cacheLock)
            {
                Thread.Sleep(500);
                log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");
            }
        }
    }
}
