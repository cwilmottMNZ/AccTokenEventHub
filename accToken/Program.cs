using System;
using System.Collections;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Identity.Client;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;


namespace EventHubsSenderReceiverRbac
{
    class Program
    {
        static readonly string TenantId = System.Environment.GetEnvironmentVariable("AZURE_TENANT_ID");
        static readonly string ClientId = System.Environment.GetEnvironmentVariable("AZURE_CLIENT_ID");
        static readonly string EventHubNamespace = ConfigurationManager.AppSettings["eventHubNamespaceFQDN"];
        static readonly string EventHubName = ConfigurationManager.AppSettings["eventHubName"];
        private const int numOfEvents = 5;
        static readonly string ClientSecret = System.Environment.GetEnvironmentVariable("AZURE_CLIENT_SECRET");

        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=accpocdevchris;AccountKey=J9MGJtRSX6anZXdZ+Sm2nPZ0Vrrn3G9GAhAJzYVmtyUl5LG1QqqtKGl9GEa8IZJoJXoKB/h6n/kM+ASt2mY6Gw==;EndpointSuffix=core.windows.net";
        private const string blobContainerName = "accpoc-tf-dev-chris";
        static BlobContainerClient storageClient;
        static private int eventsRead = 0;
        static private int maximumEvents = 40;
        static EventProcessorClient processor;


        static async Task Main()
        {
            Console.WriteLine("Press s to send {0} events\n", numOfEvents);
            Console.WriteLine("Press r to receive events\n");
            Console.WriteLine("Press any other key to exit\n");
            var userInput = Console.ReadKey();
            Console.WriteLine("\n");
            switch (userInput.Key)
            {
                case ConsoleKey.S:
                    await sendEvents();
                    break;
                case ConsoleKey.R:
                    await receiveEvents();
                    break;
                default:
                    return;
            }
        }


        static async Task sendEvents()
        {
            var credential = new ClientSecretCredential(TenantId, ClientId, ClientSecret);
            Console.WriteLine("Sending single event");
            var credentials = new DefaultAzureCredential();
            var ehClient = new EventHubProducerClient(EventHubNamespace, EventHubName, credential);
            Console.WriteLine("Creds: {0}",credentials);
            Console.WriteLine("Client: {0}", ehClient);

            Console.WriteLine("Sending");
            using Azure.Messaging.EventHubs.Producer.EventDataBatch eventBatch = await ehClient.CreateBatchAsync();
            for (int i = 1; i <= numOfEvents; i++)
            {
                if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes("datas"))))
                {
                    // if it is too large for the batch
                    throw new Exception($"Event {i} is too large for the batch and cannot be sent.");
                }
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await ehClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of {numOfEvents} events has been published.");
            }
            finally
            {
                await ehClient.DisposeAsync();
            }
            Console.WriteLine("Send done");
        }

        static async Task receiveEvents()
        {
            var credentials = new DefaultAzureCredential();
            var credential = new ClientSecretCredential(TenantId, ClientId, ClientSecret);

            string consumerGroup = "$Default";

            // Create a blob container client that the event processor will use 
            storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

            // Create an event processor client to process events in the event hub
            processor = new EventProcessorClient(storageClient, consumerGroup, EventHubNamespace, EventHubName, credential);

            // Register handlers for processing events and handling errors
            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            // Start the processing
            await processor.StartProcessingAsync();

            // Wait for 30 seconds for the events to be processed
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Stop the processing
            await processor.StopProcessingAsync();
        }

        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            if (eventsRead == maximumEvents)
            {
                return;
            };
            // Write the body of the event to the console window
            Console.WriteLine("\tReceived event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
            eventsRead++;
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}