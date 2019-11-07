using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using System.Threading.Tasks;

namespace MessageEnqueuer
{
    class Program
    {
        static ManagementClient ManagementClient;

        static QueueClient HighPriorityQueueClient;
        static QueueClient LowPriorityQueueClient;

        const bool WithControlMessages = false;
        
        static async Task Main(string[] args)
        {
            string connectionString = new ConfigurationBuilder().AddJsonFile("local.settings.json").Build()["ConnectionStrings:ServiceBus"];

            ManagementClient = new ManagementClient(connectionString);
            
            HighPriorityQueueClient = new QueueClient(connectionString, "highpriority");
            LowPriorityQueueClient = new QueueClient(connectionString, "lowpriority");


            for (var i = 0; i < 100; i++)
            {
                if (i % 25 == 0)
                {
                    if (WithControlMessages)
                    {
                        await EnqueueHighMessageWithControlMessages(i).ConfigureAwait(false);
                    }
                    else
                    {
                        await EnqueueHighMessage(i).ConfigureAwait(false);

                    }
                }

                await EnqueueLowMessage(i).ConfigureAwait(false);
            }
        }

        static async Task EnqueueHighMessage(int i)
        {
            await HighPriorityQueueClient.SendAsync(GenerateMessage($"high priority message {i}")).ConfigureAwait(false);
        }

        static async Task EnqueueHighMessageWithControlMessages(int i)
        {
            var lowPriorityQueueDescription = await ManagementClient.GetQueueAsync("lowpriority").ConfigureAwait(false);
            var lowPriorityQueueIsActive = lowPriorityQueueDescription.Status == EntityStatus.Active;

            if (lowPriorityQueueIsActive)
            {
                await HighPriorityQueueClient.SendAsync(GenerateMessage("disable")).ConfigureAwait(false);
            }

            await HighPriorityQueueClient.SendAsync(GenerateMessage($"high priority message {i}")).ConfigureAwait(false);
            
            if (lowPriorityQueueIsActive)
            {
                // https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.servicebus.message.scheduledenqueuetimeutc?view=azure-dotnet#Microsoft_Azure_ServiceBus_Message_ScheduledEnqueueTimeUtc
                var delayedMessage = GenerateMessage("enable");
                delayedMessage.ScheduledEnqueueTimeUtc = DateTime.UtcNow.AddSeconds(5);
                await HighPriorityQueueClient.SendAsync(delayedMessage).ConfigureAwait(false);
            }
        }

        static Task EnqueueLowMessage(int i)
        {
            return LowPriorityQueueClient.SendAsync(GenerateMessage($"low priority message {i}"));
        }

        private static Message GenerateMessage(string str)
        {
            return new Message(Encoding.UTF8.GetBytes(str));
        }
    }
}
