namespace OrderProcessor.Models;

public record OrderMessage
{
    public Guid Id { get; set; }
    public string UserId { get; set; } = string.Empty;
    public decimal Value { get; set; }
    public DateTime CreatedDate { get; set; }
}
