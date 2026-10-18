using System.Text.Json;
using Confluent.Kafka;
using Contracts;
using Microsoft.Extensions.Options;

namespace AnalyticsService.Services;

/// <summary>
/// Consumidor de Kafka del AnalyticsService.
///
/// Es el segundo consumidor del MISMO topic que NotificationService, pero con otro
/// GroupId. Eso es el fan-out de Kafka: cada grupo recibe el 100% de los eventos.
/// Si dos instancias del AnalyticsService compartieran el GroupId, se repartirian
/// las particiones entre ellas (work sharing), no duplicarian trabajo.
///
/// Confirma offsets manualmente (EnableAutoCommit=false) y expone el estado del
/// consumidor (asignacion, offset por particion y lag) en /analytics/consumer.
/// </summary>
public sealed class AnalyticsWorker : BackgroundService
{
    private readonly KafkaConsumerOptions _kafka;
    private readonly AnalyticsOptions _options;
    private readonly MetricsAggregator _metrics;
    private readonly ILogger<AnalyticsWorker> _logger;

    private IConsumer<string, string>? _consumer;
    private List<object> _partitionLags = new();
    private readonly Dictionary<int, long> _committedOffsets = new();
    private readonly List<int> _assignedPartitions = new();
    private readonly object _stateGate = new();

    public AnalyticsWorker(
        IOptions<KafkaConsumerOptions> kafka,
        IOptions<AnalyticsOptions> options,
        MetricsAggregator metrics,
        ILogger<AnalyticsWorker> logger)
    {
        _kafka = kafka.Value;
        _options = options.Value;
        _metrics = metrics;
        _logger = logger;
    }

    public string State { get; private set; } = "starting";

protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // El loop de Consume() es sincrono: sin esta cesion el arranque del host
        // web nunca termina (ningun await queda pendiente).
        await Task.Yield();

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            GroupId = _kafka.GroupId,
            ClientId = _kafka.ClientId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = string.Equals(_kafka.OffsetReset, "Latest", StringComparison.OrdinalIgnoreCase)
                ? Confluent.Kafka.AutoOffsetReset.Latest
                : Confluent.Kafka.AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = false,
            SessionTimeoutMs = 10_000,
            MaxPollIntervalMs = 300_000
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetPartitionsAssignedHandler((_, partitions) =>
            {
                State = "running";
                lock (_stateGate)
                {
                    _assignedPartitions.Clear();
                    _assignedPartitions.AddRange(partitions.Select(p => p.Partition.Value));
                }
                _logger.LogInformation("Asignadas particiones de {Topic}: {Partitions}",
                    _kafka.Topic, string.Join(", ", _assignedPartitions));
                return partitions.Select(p => new TopicPartitionOffset(p, Offset.Unset)).ToList();
            })
            .SetPartitionsRevokedHandler((_, partitions) =>
            {
                lock (_stateGate)
                {
                    foreach (var partition in partitions) _committedOffsets.Remove(partition.Partition.Value);
                }
                _logger.LogInformation("Particiones revocadas: {Partitions}",
                    string.Join(", ", partitions.Select(p => p.ToString())));
            })
            .Build();

        _consumer = consumer;
        consumer.Subscribe(_kafka.Topic);
        _logger.LogInformation("AnalyticsService consumiendo topic={Topic} group={Group} (modo {Mode})",
            _kafka.Topic, _kafka.GroupId, _kafka.OffsetReset);

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

                if (result is null)
                {
                    // Cada vuelta sin mensajes (~1 s) se refresca el estado de lag.
                    RefreshLags();
                    continue;
                }

                var partition = result.Partition.Value;
                var offset = result.Offset.Value;

                try
                {
                    var orderEvent = JsonSerializer.Deserialize<OrderEvent>(result.Message.Value, EventJson.Options);

                    if (orderEvent is null)
                    {
                        _metrics.CountMalformed();
                        _logger.LogWarning("Evento vacio en {Topic}[{Partition}]@{Offset}", result.Topic, partition, offset);
                    }
                    else
                    {
                        if (!_metrics.TryApply(orderEvent, partition, offset))
                            _logger.LogInformation("Duplicado ignorado: {EventType} eventId={EventId}",
                                orderEvent.EventType, orderEvent.EventId);
                    }
                }
                catch (JsonException ex)
                {
                    _metrics.CountMalformed();
                    _logger.LogWarning(ex, "Evento ilegible en {Topic}[{Partition}]@{Offset}", result.Topic, partition, offset);
                }

                consumer.Commit(result);
                lock (_stateGate) _committedOffsets[partition] = offset + 1;
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
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error cerrando el consumidor.");
            }
            _logger.LogInformation("AnalyticsService detenido.");
        }
    }

    /// <summary>
    /// Estado del consumidor: que particiones tiene asignadas, hasta que offset llego
    /// y cuantos eventos le faltan (lag). El lag se calcula con los watermarks del broker.
    /// </summary>
    public object ConsumerState()
    {
        lock (_stateGate)
        {
            return new
            {
                groupId = _kafka.GroupId,
                topic = _kafka.Topic,
                state = State,
assignedPartitions = _assignedPartitions.OrderBy(p => p).ToArray(),
                processedUpTo = _committedOffsets.ToDictionary(p => p.Key.ToString(), p => p.Value)
            };
        }
    }

    public IReadOnlyList<object> PartitionLags()
    {
        // Se devuelve el ultimo estado calculado por el hilo del consumidor.
        // Consultar el consumer desde el hilo HTTP no es seguro (librdkafka no es thread-safe).
        lock (_stateGate) return _partitionLags.ToArray();
    }

    private void RefreshLags()
    {
        if (_consumer is null) return;

        var snapshot = new List<object>();

        foreach (var partition in _assignedPartitions.OrderBy(p => p))
        {
            var topicPartition = new TopicPartition(_kafka.Topic, partition);

            // El offset confirmado se le pregunta al broker: es la fuente de verdad.
            var committed = Offset.Unset;
            try
            {
                var offsets = _consumer.Committed(new[] { topicPartition }, TimeSpan.FromSeconds(2));
                if (offsets.Count > 0) committed = offsets[0].Offset;
            }
            catch (KafkaException)
            {
                // broker no disponible
            }

            long high = 0;
            long low = 0;
            try
            {
                var watermark = _consumer.QueryWatermarkOffsets(topicPartition, TimeSpan.FromSeconds(2));
                high = watermark.High.Value;
                low = watermark.Low.Value;
            }
            catch (KafkaException)
            {
                // broker no disponible: se reporta lo que se sepa
            }

            var committedOffset = committed == Offset.Unset ? -1 : committed.Value;

            snapshot.Add(new
            {
                partition,
                committedOffset,
                logStartOffset = low,
                logEndOffset = high,
                lag = committedOffset < 0 ? Math.Max(0, high - low) : Math.Max(0, high - committedOffset)
            });
        }

        lock (_stateGate) _partitionLags = snapshot;
    }
}