namespace AnalyticsService.Services;

public sealed class KafkaConsumerOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "order-events";

    /// <summary>
    /// Grupo propio de AnalyticsService. Como es un grupo DISTINTO al de
    /// NotificationService, recibe el 100% de los eventos (fan-out).
    /// Cambiar este valor por otro (por ejemplo "analytics-replay") hace que el
    /// servicio reprocese TODO el log desde el principio: eso es el replay.
    /// </summary>
    public string GroupId { get; set; } = "analytics-service";

    public string ClientId { get; set; } = "analytics-service";
    public string OffsetReset { get; set; } = "Earliest";
}

public sealed class AnalyticsOptions
{
    public const string SectionName = "Analytics";

    /// <summary>Cantidad de eventos recientes que se guardan para /analytics/recent.</summary>
    public int RecentBufferSize { get; set; } = 50;

    /// <summary>Archivo CSV opcional donde se vuelca el resumen ("" = no volcar).</summary>
    public string ReportPath { get; set; } = "";
}