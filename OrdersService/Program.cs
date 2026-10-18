using Contracts;
using OrdersService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection(KafkaOptions.SectionName));
builder.Services.AddSingleton<OrderStore>();
builder.Services.AddSingleton<OrderEventPublisher>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<OrderEventPublisher>());

var app = builder.Build();

var store = app.Services.GetRequiredService<OrderStore>();
var publisher = app.Services.GetRequiredService<OrderEventPublisher>();
var log = app.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Orders");

// -----------------------------------------------------------------------------
// GET /health
// -----------------------------------------------------------------------------
app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "orders-service",
    kafka = new
    {
        connected = publisher.IsReady,
        topic = publisher.Topic,
        bootstrapServers = builder.Configuration["Kafka:BootstrapServers"]
    },
    orders = store.Count
}));

// -----------------------------------------------------------------------------
// GET /api/catalog  -> catalogo con el stock disponible
// -----------------------------------------------------------------------------
app.MapGet("/api/catalog", () => Results.Ok(store.Catalog()));

// -----------------------------------------------------------------------------
// POST /api/orders -> crea el pedido y publica order.created
//
// La API no espera a ningun consumidor: responde 202 apenas Kafka confirma el evento.
// Si Kafka no responde, responde 503 y el pedido no se guarda (no se pierde el evento).
// -----------------------------------------------------------------------------
app.MapPost("/api/orders", async (CreateOrderRequest request, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.CustomerId))
        return Results.BadRequest(new ErrorResponse { Message = "El campo CustomerId es obligatorio." });

    if (string.IsNullOrWhiteSpace(request.ProductId))
        return Results.BadRequest(new ErrorResponse { Message = "El campo ProductId es obligatorio." });

    if (request.Quantity is < 1 or > 1000)
        return Results.BadRequest(new ErrorResponse { Message = "Quantity debe estar entre 1 y 1000." });

    if (!store.TryGetProduct(request.ProductId, out var product))
        return Results.NotFound(new ErrorResponse { Message = $"El producto {request.ProductId} no existe." });

    var now = DateTimeOffset.UtcNow;
    var email = string.IsNullOrWhiteSpace(request.CustomerEmail)
        ? $"{request.CustomerId.ToLowerInvariant()}@tp11.local"
        : request.CustomerEmail!;

    var orderEvent = OrderEvent.ForCreated(
        orderId: "ORD-" + Guid.NewGuid().ToString("N")[..8].ToUpperInvariant(),
        customerId: request.CustomerId,
        customerEmail: email,
        productId: product.ProductId,
        quantity: request.Quantity,
        unitPrice: product.UnitPrice,
        sequence: 1,
        now: now);

    EventReceipt receipt;
    try
    {
        receipt = await publisher.PublishAsync(orderEvent, ct);
    }
    catch (Exception ex)
    {
        log.LogError(ex, "No se pudo publicar {EventType}. Se devuelve 503.", orderEvent.EventType);
        return Results.Json(
            new ErrorResponse { Message = "Kafka no esta disponible: el pedido no se registro. Reintentar." },
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    var order = new Order
    {
        OrderId = orderEvent.OrderId,
        CustomerId = orderEvent.CustomerId,
        CustomerEmail = orderEvent.CustomerEmail,
        ProductId = orderEvent.ProductId,
        Quantity = orderEvent.Quantity,
        UnitPrice = orderEvent.UnitPrice,
        CreatedAt = now,
        UpdatedAt = now
    };
    store.Add(order);

    log.LogInformation("Pedido {OrderId} creado. Evento {EventType} en {Topic}[{Partition}]@{Offset}",
        order.OrderId, receipt.EventType, receipt.Topic, receipt.Partition, receipt.Offset);

    return Results.Json(new OrderWithReceipt { Order = OrderResponse.From(order), Event = receipt },
        statusCode: StatusCodes.Status202Accepted);
});

// -----------------------------------------------------------------------------
// GET /api/orders | GET /api/orders/{orderId}
// -----------------------------------------------------------------------------
app.MapGet("/api/orders", () => Results.Ok(store.All()));

app.MapGet("/api/orders/{orderId}", (string orderId) =>
    store.TryGet(orderId, out var order)
        ? Results.Ok(OrderResponse.From(order))
        : Results.NotFound(new ErrorResponse { Message = $"El pedido {orderId} no existe.", OrderId = orderId }));

// -----------------------------------------------------------------------------
// POST /api/orders/{orderId}/confirm -> publica order.confirmed
// -----------------------------------------------------------------------------
app.MapPost("/api/orders/{orderId}/confirm", async (string orderId, CancellationToken ct) =>
{
    if (!store.TryGet(orderId, out var order))
        return Results.NotFound(new ErrorResponse { Message = $"El pedido {orderId} no existe.", OrderId = orderId });

    if (order.Status != OrderStatuses.Created)
        return Results.Json(new ErrorResponse
        {
            Message = $"Transicion invalida: el pedido esta en estado {order.Status} y no se puede confirmar.",
            OrderId = orderId
        }, statusCode: StatusCodes.Status409Conflict);

    var now = DateTimeOffset.UtcNow;

    // 1) Se reserva stock de forma atomica. Si no alcanza, el pedido se rechaza y
    //    el rechazo tambien viaja por Kafka (order.rejected).
    if (!store.TryReserve(order.ProductId, order.Quantity, out var remaining))
    {
        var reason = $"Stock insuficiente. Disponible: {remaining}, solicitado: {order.Quantity}.";

        var rejectEvent = OrderEvent.ForRejected(
            order.OrderId, order.CustomerId, order.CustomerEmail, order.ProductId,
            order.Quantity, order.UnitPrice, order.Sequence + 1, now, reason);

        EventReceipt rejectReceipt;
        try
        {
            rejectReceipt = await publisher.PublishAsync(rejectEvent, ct);
        }
        catch (Exception ex)
        {
            log.LogError(ex, "No se pudo publicar order.rejected. Se devuelve 503.");
            return Results.Json(new ErrorResponse { Message = "Kafka no esta disponible: reintentar." },
                statusCode: StatusCodes.Status503ServiceUnavailable);
        }

        order.Status = OrderStatuses.Rejected;
        order.Sequence++;
        order.StateHistory.Add(OrderStatuses.Rejected);
        order.UpdatedAt = now;

        return Results.Json(new ErrorResponse { Message = reason, OrderId = orderId, Event = rejectReceipt },
            statusCode: StatusCodes.Status409Conflict);
    }

    var confirmEvent = OrderEvent.ForConfirmed(
        order.OrderId, order.CustomerId, order.CustomerEmail, order.ProductId,
        order.Quantity, order.UnitPrice, order.Sequence + 1, now);

    try
    {
        var receipt = await publisher.PublishAsync(confirmEvent, ct);

        order.Status = OrderStatuses.Confirmed;
        order.Sequence++;
        order.StateHistory.Add(OrderStatuses.Confirmed);
        order.UpdatedAt = now;

        log.LogInformation("Pedido {OrderId} confirmado. Stock restante de {ProductId}: {Remaining}",
            orderId, order.ProductId, remaining);

        return Results.Json(new OrderWithReceipt { Order = OrderResponse.From(order), Event = receipt },
            statusCode: StatusCodes.Status202Accepted);
    }
    catch (Exception ex)
    {
        // Se devuelve el stock reservado: sin esta compensacion, un fallo de Kafka
        // dejaria el inventario congelado. Es exactamente el problema que resuelve el patron outbox.
        store.Release(order.ProductId, order.Quantity);
        log.LogError(ex, "No se pudo publicar order.confirmed. Se devuelve el stock. Se devuelve 503.");
        return Results.Json(new ErrorResponse { Message = "Kafka no esta disponible: reintentar." },
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }
});

// -----------------------------------------------------------------------------
// POST /api/orders/{orderId}/ship -> publica order.shipped
// -----------------------------------------------------------------------------
app.MapPost("/api/orders/{orderId}/ship", async (string orderId, CancellationToken ct) =>
{
    if (!store.TryGet(orderId, out var order))
        return Results.NotFound(new ErrorResponse { Message = $"El pedido {orderId} no existe.", OrderId = orderId });

    if (order.Status != OrderStatuses.Confirmed)
        return Results.Json(new ErrorResponse
        {
            Message = $"Transicion invalida: el pedido esta en estado {order.Status}. Solo se envia un pedido confirmado.",
            OrderId = orderId
        }, statusCode: StatusCodes.Status409Conflict);

    var now = DateTimeOffset.UtcNow;

    var shipEvent = OrderEvent.ForShipped(
        order.OrderId, order.CustomerId, order.CustomerEmail, order.ProductId,
        order.Quantity, order.UnitPrice, order.Sequence + 1, now);

    EventReceipt receipt;
    try
    {
        receipt = await publisher.PublishAsync(shipEvent, ct);
    }
    catch (Exception ex)
    {
        log.LogError(ex, "No se pudo publicar order.shipped. Se devuelve 503.");
        return Results.Json(new ErrorResponse { Message = "Kafka no esta disponible: reintentar." },
            statusCode: StatusCodes.Status503ServiceUnavailable);
    }

    order.Status = OrderStatuses.Shipped;
    order.Sequence++;
    order.StateHistory.Add(OrderStatuses.Shipped);
    order.UpdatedAt = now;

    return Results.Json(new OrderWithReceipt { Order = OrderResponse.From(order), Event = receipt },
        statusCode: StatusCodes.Status202Accepted);
});

// -----------------------------------------------------------------------------
// GET /api/events -> historial de eventos publicados por ESTE servicio.
// Sirve para verificar, en las pruebas, que topic/partition/offset quedaron bien.
// -----------------------------------------------------------------------------
app.MapGet("/api/events", (int? take) =>
{
    var history = publisher.History();
    var filtered = take is > 0 ? history.Take(take.Value).ToArray() : history.ToArray();
    return Results.Ok(new
    {
        topic = publisher.Topic,
        total = history.Count,
        events = filtered
    });
});

app.Run();

public partial class Program { }