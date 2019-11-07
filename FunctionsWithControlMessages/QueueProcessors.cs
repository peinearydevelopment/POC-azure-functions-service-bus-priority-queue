using System;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

// https://github.com/Azure/azure-functions-host/issues/912
// https://docs.microsoft.com/en-us/azure/azure-functions/functions-host-json#singleton
// https://docs.microsoft.com/en-us/azure/azure-functions/functions-bindings-service-bus#host-json
namespace FunctionsWithControlMessages
{
    public static class QueueProcessors
    {
        [Singleton(Mode = SingletonMode.Listener)]
        [FunctionName("HighPriorityQueueMessageProcessor")]
        public async static Task Run([ServiceBusTrigger("highpriority", Connection = "ConnectionStrings:ServiceBus")]string message, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {message}");

            var connectionString = Environment.GetEnvironmentVariable("ConnectionStrings:ServiceBus");
            if (message == "enable" || message == "disable")
            {

                var managementClient = new ManagementClient(connectionString);
                var lowPriorityQueueClient = await managementClient.GetQueueAsync("lowpriority").ConfigureAwait(false);
                // https://stackoverflow.com/questions/16017387/how-to-change-the-properties-of-a-service-bus-queue
                // https://docs.microsoft.com/en-us/azure/service-bus-messaging/entity-suspend
                // https://docs.microsoft.com/en-us/rest/api/servicebus/queues/createorupdate
                lowPriorityQueueClient.Status = message == "enable" ? EntityStatus.Active : EntityStatus.ReceiveDisabled;
                await managementClient.UpdateQueueAsync(lowPriorityQueueClient).ConfigureAwait(false);
            }
        }

        [Singleton(Mode = SingletonMode.Listener)]
        [FunctionName("LowPriorityQueueMessageProcessor")]
        public static void Run2([ServiceBusTrigger("lowpriority", Connection = "ConnectionStrings:ServiceBus")]string message, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {message}");
        }
    }
}
