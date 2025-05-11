using System.Text.Json;
using Confluent.Kafka;
using OrderProcessor.Models;

namespace OrderProcessor;

public class OrderConsumer
{
    private readonly string boostrapServerUrl = "localhost:9092";

    private const string TopicName = "Order";
    private const string RetryTopic = "order-retry";
    private const string DeadLetterTopic = "order-dlt";
    private readonly HashSet<Guid> processedOrders = new();
    

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = boostrapServerUrl,
            GroupId = "order-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };
        
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(TopicName);
        
        Console.WriteLine($"Subscribed to: {TopicName}");

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = consumer.Consume(cancellationToken);
                var message = JsonSerializer.Deserialize<OrderMessage>(consumeResult.Message.Value);

                if (message is null) throw new NullReferenceException("Invalid message");

                if (processedOrders.Contains(message.Id))
                {
                    Console.WriteLine($"Order {message.Id} has already been processed");
                    continue;
                }

                if (message.Value == 13) throw new Exception("Simulated failure");

                Console.WriteLine($"Processing order {message}");
                processedOrders.Add(message.Id);
            }
            catch (ConsumeException ex)
            {
                Console.WriteLine($"Consumer error occured: {ex.Message}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occured: {ex.Message}");
                await SendToTopicAsync(ex.Message);
            }
        }
    }

    private async Task SendToTopicAsync(string exMessage)
    {
        var config = new ProducerConfig { BootstrapServers = boostrapServerUrl };
        
        using var producer = new ProducerBuilder<Null, string>(config).Build();
        await producer.ProduceAsync(DeadLetterTopic, 
            new Message<Null, string>
            {
                Value = $"Failed message: {exMessage}"
            });
        
        Console.WriteLine($"Delivered to topic: {DeadLetterTopic}");
    }
}