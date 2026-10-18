namespace Contracts;

// ---- Contratos de la API HTTP de OrdersService (no viajan por Kafka) ----

public sealed record CreateOrderRequest
{
    public string CustomerId { get; init; } = "";
    public string? CustomerEmail { get; init; }
    public string ProductId { get; init; } = "";
    public int Quantity { get; init; }
}

public sealed record OrderResponse
{
    public string OrderId { get; init; } = "";
    public string CustomerId { get; init; } = "";
    public string CustomerEmail { get; init; } = "";
    public string ProductId { get; init; } = "";
    public int Quantity { get; init; }
    public decimal UnitPrice { get; init; }
    public decimal Total { get; init; }
    public string Currency { get; init; } = "ARS";
    public string Status { get; init; } = "";
    public int Sequence { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset UpdatedAt { get; init; }
    public IReadOnlyList<string> StateHistory { get; init; } = Array.Empty<string>();

    public static OrderResponse From(Order order) => new()
    {
        OrderId = order.OrderId,
        CustomerId = order.CustomerId,
        CustomerEmail = order.CustomerEmail,
        ProductId = order.ProductId,
        Quantity = order.Quantity,
        UnitPrice = order.UnitPrice,
        Total = order.Total,
        Currency = order.Currency,
        Status = order.Status,
        Sequence = order.Sequence,
        CreatedAt = order.CreatedAt,
        UpdatedAt = order.UpdatedAt,
        StateHistory = order.StateHistory.ToArray()
    };
}

/// <summary>
/// Estado del pedido tal como lo guarda OrdersService (en memoria en esta practica).
/// </summary>
public sealed class Order
{
    public string OrderId { get; init; } = "";
    public string CustomerId { get; init; } = "";
    public string CustomerEmail { get; init; } = "";
    public string ProductId { get; init; } = "";
    public int Quantity { get; init; }
    public decimal UnitPrice { get; init; }
    public decimal Total => UnitPrice * Quantity;
    public string Currency { get; init; } = "ARS";
    public string Status { get; set; } = OrderStatuses.Created;
    public int Sequence { get; set; } = 1;
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset UpdatedAt { get; set; }
    public List<string> StateHistory { get; } = new() { OrderStatuses.Created };
}

/// <summary>
/// Acuse de publicacion: dice en que topic, particion y offset quedo el evento.
/// Es la evidencia de que el evento quedo persistido en Kafka antes de responder.
/// </summary>
public sealed record EventReceipt
{
    public string EventId { get; init; } = "";
    public string EventType { get; init; } = "";
    public string Topic { get; init; } = "";
    public int Partition { get; init; }
    public long Offset { get; init; }
    public string Key { get; init; } = "";
    public DateTimeOffset PublishedAt { get; init; }
}

public sealed record OrderWithReceipt
{
    public required OrderResponse Order { get; init; }
    public required EventReceipt Event { get; init; }
}

public sealed record ProductStockResponse
{
    public string ProductId { get; init; } = "";
    public string ProductName { get; init; } = "";
    public decimal UnitPrice { get; init; }
    public int Stock { get; init; }
}

public sealed record ErrorResponse
{
    public string Message { get; init; } = "";
    public string OrderId { get; init; } = "";
    public EventReceipt? Event { get; init; }
}