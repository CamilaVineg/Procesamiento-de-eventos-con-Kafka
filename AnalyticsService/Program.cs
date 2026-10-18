using AnalyticsService.Services;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaConsumerOptions>(builder.Configuration.GetSection(KafkaConsumerOptions.SectionName));
builder.Services.Configure<AnalyticsOptions>(builder.Configuration.GetSection(AnalyticsOptions.SectionName));
builder.Services.AddSingleton<MetricsAggregator>();
builder.Services.AddSingleton<AnalyticsWorker>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<AnalyticsWorker>());

var app = builder.Build();

var metrics = app.Services.GetRequiredService<MetricsAggregator>();
var kafka = app.Services.GetRequiredService<IOptions<KafkaConsumerOptions>>().Value;
var worker = app.Services.GetRequiredService<AnalyticsWorker>();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "analytics-service",
    consumer = new
    {
        state = worker.State,
        groupId = kafka.GroupId,
        topic = kafka.Topic,
        bootstrapServers = kafka.BootstrapServers
    },
    eventsProcessed = metrics.Snapshot()
}));

// Metricas agregadas del topic.
app.MapGet("/analytics/summary", () => Results.Ok(metrics.Snapshot()));

app.MapGet("/analytics/products", (int? take) =>
{
    var limit = Math.Clamp(take ?? 10, 1, 100);
    return Results.Ok(new
    {
        items = metrics.TopProducts(limit).Select(p => new
        {
            productId = p.ProductId,
            units = p.Units,
            revenue = p.Revenue
        })
    });
});

app.MapGet("/analytics/customers", (int? take) =>
{
    var limit = Math.Clamp(take ?? 10, 1, 100);
    return Results.Ok(new
    {
        items = metrics.TopCustomers(limit).Select(c => new
        {
            customerId = c.CustomerId,
            orders = c.Orders
        })
    });
});

// Ultimos eventos recibidos (util para verificar que el consumidor esta leyendo).
app.MapGet("/analytics/recent", (int? take) =>
{
    var limit = Math.Clamp(take ?? 20, 1, 100);
    return Results.Ok(new { items = metrics.Recent(limit) });
});

// Estado del consumidor: grupo, particiones asignadas, offsets y lag.
app.MapGet("/analytics/consumer", () => Results.Ok(new
{
    consumer = worker.ConsumerState(),
    partitions = worker.PartitionLags()
}));

app.Run();

public partial class Program { }