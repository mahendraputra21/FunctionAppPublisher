using Azure.Messaging.EventHubs.Producer;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Messaging.EventHubs;

namespace FunctionAppPublisher
{
    public class FunctionPublisher
    {
        [Function("FunctionPublisher")]
        public async Task<IActionResult> RunAsync(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", 
            Route = null)] 
            HttpRequest req, 
            ILogger<FunctionPublisher> log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody)!;
            string message = data?.message ?? "No message provided";

            // Event Hub connection string and hub name
            string connectionString = Environment.GetEnvironmentVariable("EventHubConnectionString")!;
            string eventHubName = Environment.GetEnvironmentVariable("EventHubName")!;

            // Create a producer client
            await using (var producerClient = new EventHubProducerClient(connectionString, eventHubName))
            {
                // Create a batch
                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

                // Add events to the batch
                eventBatch.TryAdd(new EventData(System.Text.Encoding.UTF8.GetBytes(message)));

                // Send the batch to the event hub
                await producerClient.SendAsync(eventBatch);
            }

            return new OkObjectResult($"Message sent to Event Hub: {message}");
        }
    }
}
