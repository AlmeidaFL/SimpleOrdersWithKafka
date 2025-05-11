using System.Text.Json;
using ApiPedidos.Models;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace ApiPedidos.Controllers;

[ApiController]
[Route("api/orders")]
public class OrderController : ControllerBase
{
    private readonly string boostrapServerUrl = "localhost:9092";
    private readonly string topicName = "order";

    [HttpPost]
    public async Task<IActionResult> CreateOrder([FromBody] Order order)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = boostrapServerUrl
        };
        
        using var producer = new ProducerBuilder<string, string>(config).Build();
        
        var message = JsonSerializer.Serialize(order);
        var result = await producer.ProduceAsync(
            topicName,
            new Message<string, string>
            {
                Key = order.UserId,
                Value = message
            });

        return Ok(new
        {
            order.Id,
            Status = "Published on Kafka",
            Partition = result.Partition.Value,
            result.Message,
        });
    }
}