﻿using System.Text.Json;
using Confluent.Kafka;
using OrderProcessor.Models;

namespace OrderProcessor;

public class OrderConsumer
{
    private readonly string boostrapServerUrl = "localhost:9092";

    private const string TopicName = "order";
    private const string RetryTopic = "order-retry";
    
    private readonly HashSet<Guid> processedOrders = new();
    
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = boostrapServerUrl,
            GroupId = "order-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(TopicName);
        
        Console.WriteLine($"Subscribed to: {TopicName}");

        while (!cancellationToken.IsCancellationRequested)
        {
            var consumeResult = consumer.Consume(cancellationToken);

            try
            {
                var message = JsonSerializer.Deserialize<OrderMessage>(consumeResult.Message.Value);

                if (message is null) throw new NullReferenceException("Invalid message");

                if (processedOrders.Contains(message.Id))
                {
                    Console.WriteLine($"Order {message.Id} has already been processed");
                    consumer.Commit(consumeResult);
                    continue;
                }

                if (message.Value == 13) throw new Exception("Simulated failure");

                Console.WriteLine($"Processing order {message}");
                processedOrders.Add(message.Id);

                consumer.Commit(consumeResult);
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

                await SendToTopicAsync(RetryTopic, failedOrder);
                Console.WriteLine($"[RETRY] Order requeued for retry");
            }
        }
    }

    private async Task SendToTopicAsync(string topic, OrderMessage orderMessage)
    {
        var config = new ProducerConfig { BootstrapServers = boostrapServerUrl };
        
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