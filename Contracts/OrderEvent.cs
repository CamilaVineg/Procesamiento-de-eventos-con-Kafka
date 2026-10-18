namespace Contracts;

/// <summary>
/// Nombres de evento y estados del pedido.
/// Son parte del contrato publico: cambiar un string de aqui rompe a los consumidores.
/// </summary>
public static class OrderEventTypes
{
    public const string Created = "order.created";
    public const string Confirmed = "order.confirmed";
    public const string Shipped = "order.shipped";
    public const string Rejected = "order.rejected";
}

public static class OrderStatuses
{
    public const string Created = "Creado";
    public const string Confirmed = "Confirmado";
    public const string Shipped = "Enviado";
    public const string Rejected = "Rechazado";
    public const string Cancelled = "Cancelado";
}

/// <summary>
/// Evento de cambio de estado del pedido. Es lo unico que viaja por el topic
/// <c>order-events</c>. Todos los consumidores (NotificationService, AnalyticsService)
/// leen exactamente esta misma forma, asi que agregar un campo opcional es seguro:
/// los consumidores viejos lo ignoran.
/// </summary>
public sealed record OrderEvent
{
    /// <summary>Identificador unico del evento. Sirve para deduplicar (idempotencia del consumidor).</summary>
    public string EventId { get; init; } = "";

    /// <summary>Tipo de evento: order.created / order.confirmed / order.shipped / order.rejected.</summary>
    public string EventType { get; init; } = "";

    /// <summary>Version del esquema. Sube solo si el cambio es incompatible.</summary>
    public int EventVersion { get; init; } = 1;

    /// <summary>Instante en que ocurrio el cambio de estado (UTC).</summary>
    public DateTimeOffset OccurredAt { get; init; }

    /// <summary>Servicio que produce el evento.</summary>
    public string Producer { get; init; } = "";

    public string OrderId { get; init; } = "";
    public string CustomerId { get; init; } = "";
    public string CustomerEmail { get; init; } = "";
    public string ProductId { get; init; } = "";
    public int Quantity { get; init; }

    /// <summary>Precio unitario en el momento de crear el pedido.</summary>
    public decimal UnitPrice { get; init; }

    public decimal Total { get; init; }
    public string Currency { get; init; } = "ARS";

    /// <summary>Estado resultante del cambio.</summary>
    public string Status { get; init; } = "";

    /// <summary>Numero de cambio dentro del ciclo de vida del pedido (1 = creado, 2 = confirmado, 3 = enviado).</summary>
    public int Sequence { get; init; }

    /// <summary>Motivo, solo para order.rejected.</summary>
    public string? Reason { get; init; }

    public static OrderEvent ForCreated(string orderId, string customerId, string customerEmail,
        string productId, int quantity, decimal unitPrice, int sequence, DateTimeOffset now) => new()
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = OrderEventTypes.Created,
            OccurredAt = now,
            Producer = "orders-service",
            OrderId = orderId,
            CustomerId = customerId,
            CustomerEmail = customerEmail,
            ProductId = productId,
            Quantity = quantity,
            UnitPrice = unitPrice,
            Total = unitPrice * quantity,
            Status = OrderStatuses.Created,
            Sequence = sequence
        };

    public static OrderEvent ForConfirmed(string orderId, string customerId, string customerEmail,
        string productId, int quantity, decimal unitPrice, int sequence, DateTimeOffset now) => new()
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = OrderEventTypes.Confirmed,
            OccurredAt = now,
            Producer = "orders-service",
            OrderId = orderId,
            CustomerId = customerId,
            CustomerEmail = customerEmail,
            ProductId = productId,
            Quantity = quantity,
            UnitPrice = unitPrice,
            Total = unitPrice * quantity,
            Status = OrderStatuses.Confirmed,
            Sequence = sequence
        };

    public static OrderEvent ForShipped(string orderId, string customerId, string customerEmail,
        string productId, int quantity, decimal unitPrice, int sequence, DateTimeOffset now) => new()
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = OrderEventTypes.Shipped,
            OccurredAt = now,
            Producer = "orders-service",
            OrderId = orderId,
            CustomerId = customerId,
            CustomerEmail = customerEmail,
            ProductId = productId,
            Quantity = quantity,
            UnitPrice = unitPrice,
            Total = unitPrice * quantity,
            Status = OrderStatuses.Shipped,
            Sequence = sequence
        };

    public static OrderEvent ForRejected(string orderId, string customerId, string customerEmail,
        string productId, int quantity, decimal unitPrice, int sequence, DateTimeOffset now, string reason) => new()
        {
            EventId = Guid.NewGuid().ToString("N"),
            EventType = OrderEventTypes.Rejected,
            OccurredAt = now,
            Producer = "orders-service",
            OrderId = orderId,
            CustomerId = customerId,
            CustomerEmail = customerEmail,
            ProductId = productId,
            Quantity = quantity,
            UnitPrice = unitPrice,
            Total = unitPrice * quantity,
            Status = OrderStatuses.Rejected,
            Sequence = sequence,
            Reason = reason
        };
}