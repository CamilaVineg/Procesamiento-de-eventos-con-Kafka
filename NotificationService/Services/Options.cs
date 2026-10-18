namespace NotificationService.Services;

public sealed class KafkaConsumerOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "order-events";

    /// <summary>Topic donde se republican los eventos que fallaron, para reintentar mas tarde.</summary>
    public string RetryTopic { get; set; } = "order-events-retry";

    /// <summary>Topic "cola muerta": eventos que agotaron los reintentos.</summary>
    public string DlqTopic { get; set; } = "order-events-dlq";

    /// <summary>
    /// Grupo de consumidores. Cada grupo recibe TODOS los eventos del topic.
    /// Si se cambia el GroupId, el consumidor arranca desde el principio del log
    /// (por eso el replay es tan simple en Kafka).
    /// </summary>
    public string GroupId { get; set; } = "notification-service";

    public string ClientId { get; set; } = "notification-service";
    public AutoOffsetResetMode OffsetReset { get; set; } = AutoOffsetResetMode.Earliest;

    /// <summary>Milisegundos de espera entre reintentos (no bloquea el consumer: el evento va al retry topic).</summary>
    public int RetryDelayMs { get; set; } = 2000;

    /// <summary>Cantidad maxima de intentos antes de mandar el evento a la DLQ.</summary>
    public int MaxAttempts { get; set; } = 3;
}

public enum AutoOffsetResetMode
{
    Earliest,
    Latest
}

public sealed class SmtpOptions
{
    public const string SectionName = "Smtp";

    public string Host { get; set; } = "localhost";
    public int Port { get; set; } = 1025;
    public string From { get; set; } = "notificaciones@tp11.local";
    public string User { get; set; } = "";
    public string Password { get; set; } = "";
}

public sealed class NotificationOptions
{
    public const string SectionName = "Notifications";

    /// <summary>Registro en JSON Lines de las notificaciones enviadas (tambien sirve para la idempotencia).</summary>
    public string FilePath { get; set; } = "data/notifications.jsonl";

    /// <summary>
    /// none       = nunca falla (modo normal)
    /// always     = siempre falla (manda todo a la DLQ)
    /// transientN = falla los primeros N intentos de cada evento y despues funciona
    /// Sirve para demostrar reintentos y DLQ en las pruebas.
    /// </summary>
    public string FailMode { get; set; } = "none";
}