using NotificationService.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaConsumerOptions>(builder.Configuration.GetSection(KafkaConsumerOptions.SectionName));
builder.Services.Configure<SmtpOptions>(builder.Configuration.GetSection(SmtpOptions.SectionName));
builder.Services.Configure<NotificationOptions>(builder.Configuration.GetSection(NotificationOptions.SectionName));

builder.Services.AddSingleton<NotificationStore>();
builder.Services.AddSingleton<NotificationWorker>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<NotificationWorker>());

var app = builder.Build();

var store = app.Services.GetRequiredService<NotificationStore>();
var kafkaOptions = app.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<KafkaConsumerOptions>>().Value;
var smtpOptions = app.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<SmtpOptions>>().Value;
var notificationOptions = app.Services.GetRequiredService<Microsoft.Extensions.Options.IOptions<NotificationOptions>>().Value;
var worker = app.Services.GetRequiredService<NotificationWorker>();

app.MapGet("/health", () => Results.Ok(new
{
    status = "ok",
    service = "notification-service",
    consumer = new
    {
        state = worker?.State ?? "unknown",
        groupId = kafkaOptions.GroupId,
        topics = new[] { kafkaOptions.Topic, kafkaOptions.RetryTopic, kafkaOptions.DlqTopic },
        bootstrapServers = kafkaOptions.BootstrapServers,
        failMode = notificationOptions.FailMode,
        maxAttempts = kafkaOptions.MaxAttempts
    },
    smtp = $"{smtpOptions.Host}:{smtpOptions.Port}",
    notificationsSent = store.SentCount,
    deadLetters = store.DeadLetterCount
}));

// Notificaciones enviadas (las ultimas N, de la mas nueva a la mas vieja).
app.MapGet("/notifications", (int? take) =>
{
    var limit = Math.Clamp(take ?? 20, 1, 200);
    return Results.Ok(new
    {
        total = store.SentCount,
        items = store.Sent(limit)
    });
});

// Lo que quedo en la DLQ (agoto los reintentos).
app.MapGet("/dead-letters", (int? take) =>
{
    var limit = Math.Clamp(take ?? 20, 1, 200);
    return Results.Ok(new
    {
        total = store.DeadLetterCount,
        items = store.DeadLetters(limit)
    });
});

app.Run();

public partial class Program { }