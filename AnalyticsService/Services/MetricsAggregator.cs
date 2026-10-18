using System.Collections.Concurrent;
using Contracts;
using Microsoft.Extensions.Options;

namespace AnalyticsService.Services;

public sealed record RecentEvent(
    DateTimeOffset ReceivedAt,
    string EventType,
    string OrderId,
    string CustomerId,
    string ProductId,
    int Quantity,
    decimal Total,
    string Status,
    int Partition,
    long Offset);

/// <summary>
/// Agrega los eventos del topic en memoria y expone metricas.
/// Nota pedagogica: los agregados son ACUMULATIVOS sobre el log. Si un dia se
/// agrega un consumidor mas al mismo grupo, solo recibe la mitad de las particiones
/// (por eso cada consumidor del mismo grupo debe tener el mismo codigo de agregacion).
/// ParaAnalytics el unico consumidor del grupo, asi que ve todo.
/// </summary>
public sealed class MetricsAggregator
{
    private readonly object _gate = new();
    private readonly Dictionary<string, int> _eventsByType = new();
    private readonly Dictionary<string, int> _ordersByCustomer = new();
    private readonly Dictionary<string, (int Units, decimal Revenue)> _byProduct = new();
    private readonly Dictionary<string, string> _lastStatusByOrder = new();
    private readonly Queue<RecentEvent> _recent = new();
    private readonly HashSet<string> _seenEventIds = new();
    private readonly int _recentBufferSize;

    public MetricsAggregator(IOptions<AnalyticsOptions> options)
    {
        _recentBufferSize = Math.Max(1, options.Value.RecentBufferSize);
    }

    public long TotalEvents { get; private set; }
    public long DuplicatesIgnored { get; private set; }
    public long MalformedEvents => Interlocked.Read(ref _malformed);
    public long OrdersCreated { get; private set; }
    public long OrdersConfirmed { get; private set; }
    public long OrdersShipped { get; private set; }
    public long OrdersRejected { get; private set; }
    public long ConfirmedUnits { get; private set; }
    public decimal ConfirmedRevenue { get; private set; }
    public decimal ShippedRevenue { get; private set; }
    public DateTimeOffset? FirstEventAt { get; private set; }
    public DateTimeOffset LastEventAt { get; private set; }

    /// <summary>
    /// Devuelve false si el EventId ya estaba visto (idempotencia del consumidor).
    /// </summary>
    public bool TryApply(OrderEvent orderEvent, int partition, long offset)
    {
        lock (_gate)
        {
            if (!_seenEventIds.Add(orderEvent.EventId))
            {
                DuplicatesIgnored++;
                return false;
            }

            TotalEvents++;
            FirstEventAt ??= orderEvent.OccurredAt;
            LastEventAt = orderEvent.OccurredAt;

            _eventsByType.TryGetValue(orderEvent.EventType, out var typeCount);
            _eventsByType[orderEvent.EventType] = typeCount + 1;

            _ordersByCustomer.TryGetValue(orderEvent.CustomerId, out var customerOrders);
            if (orderEvent.EventType == OrderEventTypes.Created)
                _ordersByCustomer[orderEvent.CustomerId] = customerOrders + 1;

            _lastStatusByOrder[orderEvent.OrderId] = orderEvent.Status;

            switch (orderEvent.EventType)
            {
                case OrderEventTypes.Created:
                    OrdersCreated++;
                    break;
                case OrderEventTypes.Confirmed:
                    OrdersConfirmed++;
                    ConfirmedUnits += orderEvent.Quantity;
                    ConfirmedRevenue += orderEvent.Total;
                    AddProduct(orderEvent.ProductId, orderEvent.Quantity, orderEvent.Total);
                    break;
                case OrderEventTypes.Shipped:
                    OrdersShipped++;
                    ShippedRevenue += orderEvent.Total;
                    break;
                case OrderEventTypes.Rejected:
                    OrdersRejected++;
                    break;
            }

            _recent.Enqueue(new RecentEvent(
                DateTimeOffset.UtcNow, orderEvent.EventType, orderEvent.OrderId, orderEvent.CustomerId,
                orderEvent.ProductId, orderEvent.Quantity, orderEvent.Total, orderEvent.Status, partition, offset));

            while (_recent.Count > _recentBufferSize) _recent.Dequeue();

            return true;
        }
    }

    public void CountMalformed() => Interlocked.Increment(ref _malformed);

    private long _malformed;

    private void AddProduct(string productId, int units, decimal revenue)
    {
        _byProduct.TryGetValue(productId, out var current);
        _byProduct[productId] = (current.Units + units, current.Revenue + revenue);
    }

    public object Snapshot()
    {
        lock (_gate)
        {
            return new
            {
                totalEvents = TotalEvents,
                duplicatesIgnored = DuplicatesIgnored,
                malformedEvents = MalformedEvents,
                firstEventAt = FirstEventAt,
                lastEventAt = LastEventAt,
                orders = new
                {
                    created = OrdersCreated,
                    confirmed = OrdersConfirmed,
                    shipped = OrdersShipped,
                    rejected = OrdersRejected,
                    distinctCustomers = _ordersByCustomer.Count,
                    distinctOrders = _lastStatusByOrder.Count
                },
                revenue = new
                {
                    currency = "ARS",
                    confirmed = ConfirmedRevenue,
                    shipped = ShippedRevenue,
                    confirmedUnits = ConfirmedUnits,
                    averageTicket = OrdersConfirmed == 0 ? 0m : Math.Round(ConfirmedRevenue / OrdersConfirmed, 2)
                },
                eventsByType = _eventsByType,
                unitsByProduct = _byProduct.ToDictionary(p => p.Key, p => p.Value.Units),
                revenueByProduct = _byProduct.ToDictionary(p => p.Key, p => p.Value.Revenue),
                ordersByCustomer = _ordersByCustomer,
                ordersInFlight = _lastStatusByOrder.Count(o => o.Value != OrderStatuses.Shipped
                                                               && o.Value != OrderStatuses.Rejected),
                lastStatusByOrder = _lastStatusByOrder
            };
        }
    }

    public IReadOnlyList<RecentEvent> Recent(int take) => _recent.TakeLast(take).Reverse().ToArray();

    public IReadOnlyList<(string ProductId, int Units, decimal Revenue)> TopProducts(int take)
    {
        lock (_gate)
        {
            return _byProduct
                .OrderByDescending(p => p.Value.Units)
                .ThenBy(p => p.Key)
                .Take(take)
                .Select(p => (p.Key, p.Value.Units, p.Value.Revenue))
                .ToArray();
        }
    }

    public IReadOnlyList<(string CustomerId, int Orders)> TopCustomers(int take)
    {
        lock (_gate)
        {
            return _ordersByCustomer
                .OrderByDescending(c => c.Value)
                .ThenBy(c => c.Key)
                .Take(take)
                .Select(c => (c.Key, c.Value))
                .ToArray();
        }
    }
}