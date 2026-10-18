using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Contracts;
using MailKit.Net.Smtp;
using MailKit.Security;
using MimeKit;
using Microsoft.Extensions.Options;

namespace NotificationService.Services;

/// <summary>
/// Consumidor de Kafka del NotificationService.
///
/// Como funciona el ciclo completo:
///  1. Consume order-events con el grupo "notification-service".
///  2. Traduce el evento a un correo y lo envia por SMTP (Mailpit en Docker).
///  3. Solo despues de enviar confirma el offset (Commit). Si el proceso muere entre
///     (2) y (3), el evento vuelve a entregarse: por eso el store deduplica por EventId.
///  4. Si el envio falla, publica el evento en order-events-retry con el header
///     x-retry-count y confirma el offset original. Al agotar los intentos, lo publica
///     en order-events-dlq. Es el equivalente a la DLQ de RabbitMQ, pero armada con topics.
///  5. Si el servicio esta caido, Kafka NO pierde nada: los eventos quedan en el log
///     con la particion asignada y se procesan al volver (mismo comportamiento que una
///     cola durable de RabbitMQ).
/// </summary>
public sealed class NotificationWorker : BackgroundService
{
    private readonly KafkaConsumerOptions _kafka;
    private readonly SmtpOptions _smtp;
    private readonly NotificationOptions _options;
    private readonly NotificationStore _store;
    private readonly ILogger<NotificationWorker> _logger;

    private readonly Dictionary<string, int> _attemptsByEvent = new();
    private readonly Dictionary<string, int> _failuresByEvent = new();
    private IProducer<string, string>? _producer;
    private readonly object _producerGate = new();

    public NotificationWorker(
        IOptions<KafkaConsumerOptions> kafka,
        IOptions<SmtpOptions> smtp,
        IOptions<NotificationOptions> options,
        NotificationStore store,
        ILogger<NotificationWorker> logger)
    {
        _kafka = kafka.Value;
        _smtp = smtp.Value;
        _options = options.Value;
        _store = store;
        _logger = logger;
    }

    public string State { get; private set; } = "starting";

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // El loop de Consume() es sincrono: sin esta cesion el arranque del host
        // web puede quedar bloqueado.
        await Task.Yield();

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.GroupId,
            ClientId = _kafka.ClientId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = _kafka.OffsetReset == AutoOffsetResetMode.Earliest
                ? Confluent.Kafka.AutoOffsetReset.Earliest
                : Confluent.Kafka.AutoOffsetReset.Latest,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = 10_000,
            MaxPollIntervalMs = 300_000,
            EnablePartitionEof = false
        };

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            ClientId = $"{_kafka.ClientId}-producer",
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 10_000
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                State = "running";
                _logger.LogInformation("Asignadas {Count} particiones de {Topic}: {Partitions}",
                    partitions.Count,
                    string.Join(", ", partitions.Select(p => p.ToString())),
                    string.Join(", ", partitions.Select(p => p.ToString())));
                return partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)).ToList();
            })
            .SetPartitionsRevokedHandler((_, _) => _logger.LogInformation("Particiones revocadas (rebalanceo)."))
            .Build();

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();

        consumer.Subscribe(new[] { _kafka.Topic, _kafka.RetryTopic });
        _logger.LogInformation(
            "Consumidor listo. group={Group} topics=[{Topic}, {RetryTopic}] smtp={Host}:{Port} failMode={FailMode} maxAttempts={Max}",
            _kafka.GroupId, _kafka.Topic, _kafka.RetryTopic, _smtp.Host, _smtp.Port, _options.FailMode, _kafka.MaxAttempts);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException ex)
                {
                    State = "error";
                    _logger.LogError(ex, "Error del consumidor: {Code} {Reason}", ex.Error.Code, ex.Error.Reason);
                    await Task.Delay(2000, stoppingToken);
                    continue;
                }

                if (result is null) continue;

                await HandleAsync(consumer, result, stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // apagado normal
        }
        catch (Exception ex)
        {
            State = "error";
            _logger.LogCritical(ex, "El consumidor de Kafka se detuvo inesperadamente.");
        }
        finally
        {
            State = "stopped";
            try
            {
                consumer.Close();   // cierra el consumer group limpiamente (leave group)
                _producer?.Flush(TimeSpan.FromSeconds(3));
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error cerrando el consumidor.");
            }
            _logger.LogInformation("Consumidor detenido.");
        }
    }

    private async Task HandleAsync(IConsumer<string, string> consumer, ConsumeResult<string, string> result,
        CancellationToken ct)
    {
        var payload = result.Message?.Value;
        var topic = result.Topic;
        var partition = result.Partition.Value;
        var offset = result.Offset.Value;
        var attempt = ReadRetryCount(result.Message?.Headers);

        OrderEvent? orderEvent = null;
        try
        {
            orderEvent = payload is null ? null : JsonSerializer.Deserialize<OrderEvent>(payload, EventJson.Options);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Mensaje ilegible en {Topic}[{Partition}]@{Offset}: va directo a la DLQ",
                topic, partition, offset);
            await PublishToDlqAsync(topic, partition, offset, payload ?? "", $"JSON invalido: {ex.Message}", attempt + 1, ct);
            consumer.Commit(result);
            return;
        }

        if (orderEvent is null)
        {
            _logger.LogWarning("Mensaje vacio en {Topic}[{Partition}]@{Offset}: descartado", topic, partition, offset);
            consumer.Commit(result);
            return;
        }

        try
        {
            if (_store.IsProcessed(orderEvent.EventId))
            {
                _store.CountDuplicate();
                _logger.LogInformation("Duplicado descartado (idempotencia): {EventType} {OrderId} eventId={EventId}",
                    orderEvent.EventType, orderEvent.OrderId, orderEvent.EventId);
                consumer.Commit(result);
                return;
            }

            var notification = BuildNotification(orderEvent);
            await SendEmailAsync(orderEvent, notification.To, notification.Subject, notification.Body, ct);

            // Solo despues del envio exitoso se marca como procesado.
            _store.MarkProcessed(orderEvent.EventId);

            _store.RecordSent(new NotificationRecord(
                orderEvent.EventId, orderEvent.EventType, orderEvent.OrderId, orderEvent.CustomerId,
                notification.To, notification.Subject, DateTimeOffset.UtcNow,
                topic, partition, offset, attempt + 1));

            _logger.LogInformation("Notificado: {EventType} {OrderId} -> {To} (offset {Topic}[{Partition}]@{Offset})",
                orderEvent.EventType, orderEvent.OrderId, notification.To, topic, partition, offset);

            consumer.Commit(result);
        }
        catch (Exception ex)
        {
            var currentAttempt = IncrementAttempt(orderEvent.EventId, attempt);

            if (currentAttempt >= _kafka.MaxAttempts)
            {
                _logger.LogError(ex, "Evento {EventId} agoto {Attempts} intentos -> DLQ {Dlq}",
                    orderEvent.EventId, currentAttempt, _kafka.DlqTopic);
                await PublishToDlqAsync(topic, partition, offset, payload ?? "", ex.Message, currentAttempt, ct);
                _store.RecordDeadLetter(new DeadLetterRecord(
                    orderEvent.EventId, orderEvent.EventType, orderEvent.OrderId, payload ?? "", ex.Message,
                    currentAttempt, DateTimeOffset.UtcNow, topic, partition, offset));
            }
            else
            {
                _logger.LogWarning(ex, "Fallo el envio de {EventId} (intento {Current}/{Max}). Se republica en {Retry}",
                    orderEvent.EventId, currentAttempt, _kafka.MaxAttempts, _kafka.RetryTopic);
                await PublishToRetryAsync(orderEvent, payload ?? "", currentAttempt, ex.Message, ct);
            }

            // El offset del evento original se confirma igual: el evento ya esta en
            // retry o DLQ, asi que no se va a reprocesar en loop.
            consumer.Commit(result);
        }
    }

    private int IncrementAttempt(string eventId, int fromHeader)
    {
        lock (_attemptsByEvent)
        {
            _attemptsByEvent.TryGetValue(eventId, out var stored);
            var next = Math.Max(stored, fromHeader) + 1;
            _attemptsByEvent[eventId] = next;
            return next;
        }
    }

    private static int ReadRetryCount(Headers? headers)
    {
        if (headers is null) return 0;
        if (headers.TryGetLastBytes("x-retry-count", out var bytes) && bytes is not null)
        {
            var text = Encoding.UTF8.GetString(bytes);
            if (int.TryParse(text, out var value)) return value;
        }
        return 0;
    }

    private (string To, string Subject, string Body) BuildNotification(OrderEvent orderEvent)
    {
        var to = string.IsNullOrWhiteSpace(orderEvent.CustomerEmail)
            ? $"{orderEvent.CustomerId.ToLowerInvariant()}@tp11.local"
            : orderEvent.CustomerEmail;

        var subject = orderEvent.EventType switch
        {
            OrderEventTypes.Created => $"Pedido {orderEvent.OrderId} recibido",
            OrderEventTypes.Confirmed => $"Pedido {orderEvent.OrderId} confirmado",
            OrderEventTypes.Shipped => $"Tu pedido {orderEvent.OrderId} esta en camino",
            OrderEventTypes.Rejected => $"Pedido {orderEvent.OrderId} rechazado",
            _ => $"Pedido {orderEvent.OrderId}: {orderEvent.EventType}"
        };

        var body = $"""
            Hola {orderEvent.CustomerId},

            Evento      : {orderEvent.EventType}
            Pedido      : {orderEvent.OrderId}
            Producto    : {orderEvent.ProductId} x {orderEvent.Quantity}
            Total       : {orderEvent.Total:N2} {orderEvent.Currency}
            Estado      : {orderEvent.Status}
            Secuencia   : {orderEvent.Sequence}
            Ocurrio     : {orderEvent.OccurredAt:yyyy-MM-dd HH:mm:ss} UTC
            EventId     : {orderEvent.EventId}
            {(orderEvent.Reason is null ? "" : $"Motivo       : {orderEvent.Reason}")}

            --
            Este mensaje lo genero NotificationService consumiendo el topic
            '{orderEvent.EventType}' de Kafka.
            """;

        return (to, subject, body);
    }

    private async Task SendEmailAsync(OrderEvent orderEvent, string to, string subject, string body,
        CancellationToken ct)
    {
        MaybeFail(orderEvent.EventId, to);

        using var client = new SmtpClient();
        await client.ConnectAsync(_smtp.Host, _smtp.Port, SecureSocketOptions.None, ct);

        if (!string.IsNullOrWhiteSpace(_smtp.User))
            await client.AuthenticateAsync(_smtp.User, _smtp.Password, ct);

        var message = new MimeMessage();
        message.From.Add(new MailboxAddress("TP11 - Orders", _smtp.From));
        message.To.Add(new MailboxAddress(to, to));
        message.Subject = subject;
        message.Body = new TextPart("plain") { Text = body };

        await client.SendAsync(message, ct);
        await client.DisconnectAsync(true);
    }

    /// <summary>
    /// Simulacion de fallas controlada por la variable Notifications__FailMode.
    /// Permite demostrar, sin romper nada a mano, los reintentos via topic y la DLQ.
    /// </summary>
    private void MaybeFail(string eventId, string to)
    {
        var mode = _options.FailMode?.Trim() ?? "none";

        if (mode.Equals("none", StringComparison.OrdinalIgnoreCase)) return;

        if (mode.Equals("always", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException($"SMTP no disponible (FAIL_MODE=always) al enviar a {to}");

        if (mode.StartsWith("transient", StringComparison.OrdinalIgnoreCase)
            && int.TryParse(mode.AsSpan("transient".Length), out var times))
        {
            lock (_failuresByEvent)
            {
                _failuresByEvent.TryGetValue(eventId, out var failures);
                if (failures < times)
                {
                    _failuresByEvent[eventId] = failures + 1;
                    throw new InvalidOperationException(
                        $"SMTP intermitente (FAIL_MODE=transient{times}): fallo {failures + 1} de {times}");
                }
            }
        }
    }

    private async Task PublishToRetryAsync(OrderEvent orderEvent, string payload, int attempt, string error,
        CancellationToken ct)
    {
        var producer = Producer;
        var headers = new Headers
        {
            { "x-retry-count", Encoding.UTF8.GetBytes(attempt.ToString()) },
            { "x-original-topic", Encoding.UTF8.GetBytes(_kafka.Topic) },
            { "x-error", Encoding.UTF8.GetBytes(error.Length > 300 ? error[..300] : error) }
        };

        var message = new Message<string, string>
        {
            Key = orderEvent.OrderId,
            Value = payload,
            Headers = headers
        };

        await producer.ProduceAsync(_kafka.RetryTopic, message, ct);
        _logger.LogInformation("Evento {EventId} republicado en {Retry} (x-retry-count={Attempt})",
            orderEvent.EventId, _kafka.RetryTopic, attempt);

        await Task.Delay(_kafka.RetryDelayMs, CancellationToken.None);
    }

    private async Task PublishToDlqAsync(string sourceTopic, int partition, long offset, string payload, string error,
        int attempts, CancellationToken ct)
    {
        var headers = new Headers
        {
            { "x-attempts", Encoding.UTF8.GetBytes(attempts.ToString()) },
            { "x-original-topic", Encoding.UTF8.GetBytes(sourceTopic) },
            { "x-original-partition", Encoding.UTF8.GetBytes(partition.ToString()) },
            { "x-original-offset", Encoding.UTF8.GetBytes(offset.ToString()) },
            { "x-error", Encoding.UTF8.GetBytes(error.Length > 300 ? error[..300] : error) }
        };

        var message = new Message<string, string> { Value = payload, Headers = headers };
        await Producer.ProduceAsync(_kafka.DlqTopic, message, ct);
    }

    private IProducer<string, string> Producer
    {
        get
        {
            lock (_producerGate)
            {
                return _producer ?? throw new InvalidOperationException("El producer de retry/DLQ no esta inicializado.");
            }
        }
    }
}