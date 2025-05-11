namespace ApiPedidos.Models;

public class Order
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string UserId { get; set; } = string.Empty;
    public decimal Value { get; set; }
    public DateTime CreatedDate { get; set; } = DateTime.UtcNow;
}