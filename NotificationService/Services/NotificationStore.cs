using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace NotificationService.Services;

public sealed record NotificationRecord(
    string EventId,
    string EventType,
    string OrderId,
    string CustomerId,
    string To,
    string Subject,
    DateTimeOffset SentAt,
    string SourceTopic,
    int SourcePartition,
    long SourceOffset,
    int Attempt);

public sealed record DeadLetterRecord(
    string EventId,
    string EventType,
    string OrderId,
    string RawPayload,
    string Error,
    int Attempts,
    DateTimeOffset FailedAt,
    string SourceTopic,
    int SourcePartition,
    long SourceOffset);

/// <summary>
/// Registro de notificaciones enviadas y de eventos que cayeron en la DLQ.
/// Persiste en JSON Lines para sobrevivir a un reinicio del contenedor: eso permite
/// mantener la idempotencia (no volver a enviar el mismo correo) aun despues de un crash.
/// </summary>
public sealed class NotificationStore
{
    private readonly ConcurrentDictionary<string, byte> _processed = new();
    private readonly List<NotificationRecord> _sent = new();
    private readonly List<DeadLetterRecord> _deadLetters = new();
    private readonly object _fileGate = new();
    private readonly NotificationOptions _options;
    private readonly ILogger<NotificationStore> _logger;

    public NotificationStore(IOptions<NotificationOptions> options, ILogger<NotificationStore> logger)
    {
        _options = options.Value;
        _logger = logger;
        LoadProcessed();
    }

    /// <summary>
    /// True si el evento ya fue notificado antes. Sirve para descartar duplicados:
    /// Kafka garantiza entrega al menos una vez, no exactamente una.
    /// </summary>
    public bool IsProcessed(string eventId) => _processed.ContainsKey(eventId);

    /// <summary>Se marca recien despues de enviar el correo con exito.</summary>
    public void MarkProcessed(string eventId) => _processed.TryAdd(eventId, 0);

    public void RecordSent(NotificationRecord record)
    {
        lock (_fileGate)
        {
            _sent.Add(record);
            try
            {
                var dir = Path.GetDirectoryName(_options.FilePath);
                if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
                File.AppendAllLines(_options.FilePath, new[] { System.Text.Json.JsonSerializer.Serialize(record) });
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "No se pudo persistir la notificacion en {Path}", _options.FilePath);
            }
        }
    }

    public void RecordDeadLetter(DeadLetterRecord record)
    {
        lock (_fileGate) _deadLetters.Add(record);
    }

    public IReadOnlyList<NotificationRecord> Sent(int take) => _sent.TakeLast(take).Reverse().ToArray();

    public IReadOnlyList<DeadLetterRecord> DeadLetters(int take) => _deadLetters.TakeLast(take).Reverse().ToArray();

    public int SentCount => _sent.Count;

    public int DeadLetterCount => _deadLetters.Count;

    public int DuplicateCount => _duplicateCount;

    private int _duplicateCount;

    public void CountDuplicate() => Interlocked.Increment(ref _duplicateCount);

    private void LoadProcessed()
    {
        if (!File.Exists(_options.FilePath)) return;

        try
        {
            var ids = File.ReadAllLines(_options.FilePath)
                .Where(line => !string.IsNullOrWhiteSpace(line))
                .Select(line => System.Text.Json.JsonSerializer.Deserialize<NotificationRecord>(line))
                .Where(record => record is not null)
                .Select(record => record!.EventId)
                .Distinct();

            var count = 0;
            foreach (var id in ids)
            {
                _processed.TryAdd(id, 0);
                count++;
            }

            _logger.LogInformation("Registro de notificaciones cargado: {Count} eventos ya notificados.", count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "No se pudo leer {Path}", _options.FilePath);
        }
    }
}