using System.Text.Json;
using Confluent.Kafka;
using RetryProcessor.Models;

namespace RetryProcessor;

public class RetryProcessor
{
    private const string BoostrapServerUrl = "localhost:9092";
    private const string GroupId = "retry-processor-group";
    private const string DeadLetterTopic = "order-dlt";
    private const string RetryTopic = "order-retry";
    private const string OrderTopic = "order";
    private const int MaxRetries = 3;
    
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BoostrapServerUrl,
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        
        var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(RetryTopic);

        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(cancellationToken);

            try
            {
                var message = JsonSerializer.Deserialize<OrderMessage>(consumeResult.Message.Value);

                if (message is null) throw new Exception($"Not a valid schema for {nameof(OrderMessage)}");

                message.RetryCount += 1;

                if (message.RetryCount >= MaxRetries)
                {
                    await SendToTopicAsync(DeadLetterTopic, message);
                }
                else
                {
                    await SendToTopicAsync(OrderTopic, message);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] {ex.Message}");

                var failedOrder = JsonSerializer.Deserialize<OrderMessage>(consumeResult.Message.Value);

                if (failedOrder == null)
                {
                    Console.WriteLine("Invalid message schema");
                    return;
                }

                await SendToTopicAsync(DeadLetterTopic, failedOrder);
            }
        }
    }
    
    private async Task SendToTopicAsync(string topic, OrderMessage orderMessage)
    {
        var config = new ProducerConfig { BootstrapServers = BoostrapServerUrl };
        
        using var producer = new ProducerBuilder<string, string>(config).Build();
        await producer.ProduceAsync(topic, 
            new Message<string, string>
            {
                Key = orderMessage.UserId,
                Value = JsonSerializer.Serialize(orderMessage)
            });
        
        Console.WriteLine($"Delivered to topic: {topic}");
    }
}