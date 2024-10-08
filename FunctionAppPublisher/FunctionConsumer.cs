using Azure.Messaging.EventHubs.Consumer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text;

namespace FunctionAppPublisher
{
    public class FunctionConsumer
    {
        [Function("FunctionConsumer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)]
            HttpRequest req,
            ILogger<FunctionConsumer> log)
        {
            log.LogInformation("C# HTTP trigger function processed a request to consume events.");

            // Event Hub connection string and hub name
            string connectionString = Environment.GetEnvironmentVariable("EventHubConnectionString")!;
            string eventHubName = Environment.GetEnvironmentVariable("EventHubName")!;
            string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

            // Create a consumer client
            await using var consumerClient = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName);
            List<string> messages = [];

            try
            {
                // Use CancellationTokenSource to handle the timeout
                using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));

                // Read events from the Event Hub
                await foreach (PartitionEvent partitionEvent in consumerClient.ReadEventsAsync(cancellationSource.Token))
                {
                    string message = Encoding.UTF8.GetString(partitionEvent.Data.Body.ToArray());
                    messages.Add(message);
                    log.LogInformation($"Received message: {message}");
                }
            }
            catch (TaskCanceledException ex)
            {
                log.LogWarning(ex, "Event reading was canceled or timed out.");
            }
            catch (Exception ex)
            {
                log.LogError(ex, "An error occurred while reading events from the Event Hub.");
            }

            return new OkObjectResult($"Received {messages.Count} messages: {string.Join(", ", messages)}");
        }
    }
}
