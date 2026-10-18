using System.Text.Json;
using Confluent.Kafka;
using Contracts;
using Microsoft.Extensions.Options;

namespace OrdersService.Services;

/// <summary>
/// Productor de Kafka de OrdersService.
///
/// Decisiones que se ven en el codigo y que importan en la comparacion con RabbitMQ:
///  - Productor idempotente (EnableIdempotence) + acks=all: el broker no acepta el
///    mensaje hasta que esta replicado. Si se pierde la confirmacion, Kafka reintenta
///    sin duplicar el evento (deduplica por PID+secuencia).
///  - Key = OrderId: todos los cambios de estado de un mismo pedido caen en la misma
///    particion y por lo tanto se leen en orden.
///  - El envio es sincrono desde el punto de vista de la API (ProduceAsync espera el ack),
///    asi que si Kafka no esta disponible el endpoint devuelve 503 y el pedido NO queda
///    confirmado: no se pierde ningun evento.
/// </summary>
public sealed class OrderEventPublisher : IHostedService, IDisposable
{
    private readonly KafkaOptions _options;
    private readonly ILogger<OrderEventPublisher> _logger;
    private readonly List<EventReceipt> _history = new();
    private readonly object _sync = new();
    private IProducer<string, string>? _producer;

    public OrderEventPublisher(IOptions<KafkaOptions> options, ILogger<OrderEventPublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public bool IsReady => _producer is not null;

    public string Topic => _options.Topic;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            ClientId = _options.ClientId,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageSendMaxRetries = int.MaxValue,
            MaxInFlight = 5,
            LingerMs = 5,
            MessageTimeoutMs = _options.MessageTimeoutMs,
            RetryBackoffMs = _options.RetryBackoffMs,
            SocketTimeoutMs = 10_000
        };

        _producer = new ProducerBuilder<string, string>(config)
            .SetErrorHandler((_, error) =>
                _logger.LogError("Error del producer de Kafka: {Code} {Reason}", error.Code, error.Reason))
            .Build();

        _logger.LogInformation("Productor Kafka listo. topic={Topic} brokers={Brokers} idempotente=ON acks=All",
            _options.Topic, _options.BootstrapServers);

        return Task.CompletedTask;
    }

    /// <summary>
    /// Publica el evento y espera el acks del broker. Devuelve donde quedo persistido.
    /// Lanza excepcion si Kafka no responde: en ese caso la API devuelve 503.
    /// </summary>
    public async Task<EventReceipt> PublishAsync(OrderEvent orderEvent, CancellationToken cancellationToken)
    {
        var producer = _producer ?? throw new InvalidOperationException("El producer de Kafka no esta inicializado.");

        var payload = JsonSerializer.Serialize(orderEvent, EventJson.Options);

        var message = new Message<string, string>
        {
            // La clave decide la particion: mismo pedido -> misma particion -> orden garantizado.
            Key = orderEvent.OrderId,
            Value = payload,
            Headers = new Headers
            {
                { "event-type", System.Text.Encoding.UTF8.GetBytes(orderEvent.EventType) },
                { "event-id", System.Text.Encoding.UTF8.GetBytes(orderEvent.EventId) },
                { "schema-version", System.Text.Encoding.UTF8.GetBytes(orderEvent.EventVersion.ToString()) }
            }
        };

        var result = await producer.ProduceAsync(_options.Topic, message, cancellationToken);

        var receipt = new EventReceipt
        {
            EventId = orderEvent.EventId,
            EventType = orderEvent.EventType,
            Topic = result.Topic,
            Partition = result.Partition.Value,
            Offset = result.Offset.Value,
            Key = orderEvent.OrderId,
            PublishedAt = DateTimeOffset.UtcNow
        };

        lock (_sync)
        {
            _history.Add(receipt);
            if (_history.Count > 500) _history.RemoveAt(0);
        }

        _logger.LogInformation(
            "Evento publicado {EventType} order={OrderId} key={Key} -> {Topic}[{Partition}]@{Offset} ({Bytes} bytes)",
            orderEvent.EventType, orderEvent.OrderId, orderEvent.OrderId,
            result.Topic, result.Partition.Value, result.Offset.Value,
            payload.Length);

        return receipt;
    }

    public IReadOnlyList<EventReceipt> History()
    {
        lock (_sync) return _history.ToArray();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        if (_producer is null) return Task.CompletedTask;
        try
        {
            _producer.Flush(TimeSpan.FromSeconds(5));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "No se pudo hacer flush final del producer.");
        }
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        _producer?.Flush(TimeSpan.FromSeconds(3));
        _producer?.Dispose();
        _producer = null;
    }
}