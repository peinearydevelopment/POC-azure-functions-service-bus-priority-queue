using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CustomPriorityQueue
{
    // https://markheath.net/post/priority-queues-with-azure-service-bus
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = new ConfigurationBuilder().AddJsonFile("local.settings.json").Build()["ConnectionStrings:ServiceBus"];

            var managementClient = new ManagementClient(connectionString);
            
            var lowPriorityQueueClient = new QueueClient(connectionString, "lowpriority");
            var highPriorityQueueClient = new QueueClient(connectionString, "highpriority");

            lowPriorityQueueClient.RegisterMessageHandler(LowPriorityQueueMessageProcessor, new MessageHandlerOptions(ExceptionRecievedHandler) { MaxConcurrentCalls = 1 });
            highPriorityQueueClient.RegisterMessageHandler(HighPriorityQueueMessageProcessor, new MessageHandlerOptions(ExceptionRecievedHandler) { MaxConcurrentCalls = 1 });

            do
            {
            } while (true);
        }

        private static Message HighPriorityMessage;
        static Task HighPriorityQueueMessageProcessor(Message message, CancellationToken cancellationToken)
        {
            HighPriorityMessage = message;
            Console.WriteLine($"C# ServiceBus queue trigger function processed message: {Encoding.UTF8.GetString(message.Body)}");
            HighPriorityMessage = null;
            return Task.CompletedTask;
        }

        private const int DefaultRetrySeconds = 5;
        private static int RetrySeconds = DefaultRetrySeconds;
        static async Task LowPriorityQueueMessageProcessor(Message message, CancellationToken cancellationToken)
        {
            for (;;)
            {
                if (HighPriorityMessage == null)
                {
                    Console.WriteLine($"C# ServiceBus queue trigger function processed message: {Encoding.UTF8.GetString(message.Body)}");
                    RetrySeconds = DefaultRetrySeconds;
                    break;
                }

                await Task.Delay(TimeSpan.FromSeconds(RetrySeconds));
                RetrySeconds *= RetrySeconds;
            }
        }

        static Task ExceptionRecievedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Exception occurred.");
            return Task.CompletedTask;
        }
    }
}
