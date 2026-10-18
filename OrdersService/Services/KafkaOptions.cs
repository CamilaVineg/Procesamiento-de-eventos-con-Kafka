namespace OrdersService.Services;

/// <summary>
/// Seccion "Kafka" de appsettings.json / variables de entorno.
/// Variables: Kafka__BootstrapServers, Kafka__Topic.
/// </summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "order-events";
    public string ClientId { get; set; } = "orders-service";

    /// <summary>Timeout de cada envio. Si el broker no responde, el pedido no se confirma.</summary>
    public int MessageTimeoutMs { get; set; } = 10000;

    public int RetryBackoffMs { get; set; } = 250;
}