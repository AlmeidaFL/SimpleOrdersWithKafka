using OrderProcessor;

var consumer = new OrderConsumer();
await consumer.StartAsync(CancellationToken.None);