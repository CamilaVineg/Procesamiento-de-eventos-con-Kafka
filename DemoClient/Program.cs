using System.Net.Http.Json;
using System.Text.Json;
using Contracts;

// Cliente de consola que dispara el escenario completo de la consigna:
// crea pedidos, los confirma, los envia y despues muestra que receive ambos consumidores.
//
//   dotnet run --project DemoClient -- http://localhost:8080

var ordersBaseUrl = args.Length > 0 ? args[0].TrimEnd('/') : "http://localhost:8080";
var analyticsUrl = args.Length > 1 ? args[1].TrimEnd('/') : "http://localhost:8082";
var notificationUrl = args.Length > 2 ? args[2].TrimEnd('/') : "http://localhost:8081";

var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);

Console.WriteLine("TP11 - Ejercicio 3 | Cliente de demostracion (Kafka)");
Console.WriteLine($"OrdersService      : {ordersBaseUrl}");
Console.WriteLine($"AnalyticsService   : {analyticsUrl}");
Console.WriteLine($"NotificationService: {notificationUrl}");
Console.WriteLine(new string('-', 78));

await WaitForServiceAsync($"{ordersBaseUrl}/health", "OrdersService");
await WaitForServiceAsync($"{analyticsUrl}/health", "AnalyticsService");
await WaitForServiceAsync($"{notificationUrl}/health", "NotificationService");

// -----------------------------------------------------------------------------
// Escenario 1: ciclo de vida completo (creado -> confirmado -> enviado)
// -----------------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("Escenario 1 - ciclo de vida completo");
Console.WriteLine(new string('-', 78));

var pedido1 = await CrearPedidoAsync(http, ordersBaseUrl, "CLI-001", "P001", 3);
if (pedido1 is not null)
{
    await ConfirmarAsync(http, ordersBaseUrl, pedido1.Order.OrderId);
    await EnviarAsync(http, ordersBaseUrl, pedido1.Order.OrderId);
}

// -----------------------------------------------------------------------------
// Escenario 2: confirmacion rechazada por falta de stock
// -----------------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("Escenario 2 - confirmacion rechazada por stock insuficiente");
Console.WriteLine(new string('-', 78));

var pedido2 = await CrearPedidoAsync(http, ordersBaseUrl, "CLI-002", "P002", 999);
if (pedido2 is not null)
    await ConfirmarAsync(http, ordersBaseUrl, pedido2.Order.OrderId);

// -----------------------------------------------------------------------------
// Escenario 3: segundo pedido que si se envia completo (para ver metricas)
// -----------------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("Escenario 3 - segundo pedido completo");
Console.WriteLine(new string('-', 78));

var pedido3 = await CrearPedidoAsync(http, ordersBaseUrl, "CLI-003", "P003", 5);
if (pedido3 is not null)
{
    await ConfirmarAsync(http, ordersBaseUrl, pedido3.Order.OrderId);
    await EnviarAsync(http, ordersBaseUrl, pedido3.Order.OrderId);
}

// -----------------------------------------------------------------------------
// Escenario 4: transicion invalida (enviar sin confirmar)
// -----------------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("Escenario 4 - transicion invalida (enviar un pedido sin confirmar)");
Console.WriteLine(new string('-', 78));

var pedido4 = await CrearPedidoAsync(http, ordersBaseUrl, "CLI-004", "P001", 1);
if (pedido4 is not null)
    await EnviarAsync(http, ordersBaseUrl, pedido4.Order.OrderId);

// -----------------------------------------------------------------------------
// Resultado: que leyo cada consumidor
// -----------------------------------------------------------------------------
Console.WriteLine();
Console.WriteLine("Consumo de los dos consumidores del mismo topic");
Console.WriteLine(new string('-', 78));

await Task.Delay(TimeSpan.FromSeconds(3)); // margen para que los consumidores procesen

var summary = await GetJsonAsync(http, $"{analyticsUrl}/analytics/summary");
Console.WriteLine("AnalyticsService  /analytics/summary:");
Console.WriteLine(Indent(Pretty(summary)));

var consumerState = await GetJsonAsync(http, $"{analyticsUrl}/analytics/consumer");
Console.WriteLine("AnalyticsService  /analytics/consumer (asignacion, offsets y lag):");
Console.WriteLine(Indent(Pretty(consumerState)));

var notifications = await GetJsonAsync(http, $"{notificationUrl}/notifications?take=8");
Console.WriteLine("NotificationService  /notifications (correos enviados):");
Console.WriteLine(Indent(Pretty(notifications)));

Console.WriteLine();
Console.WriteLine("Cosas para mirar a mano:");
Console.WriteLine("  - correos en Mailpit:       http://localhost:8025");
Console.WriteLine("  - eventos del topic:        docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic order-events --from-beginning --max-messages 10");
Console.WriteLine("  - grupos de consumidores:   docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group analytics-service");
Console.WriteLine();

return;

async Task<OrderWithReceipt?> CrearPedidoAsync(HttpClient http, string baseUrl, string customer, string product, int qty)
{
    var response = await http.PostAsJsonAsync($"{baseUrl}/api/orders",
        new CreateOrderRequest { CustomerId = customer, ProductId = product, Quantity = qty });

    var body = await response.Content.ReadAsStringAsync();
    Console.WriteLine($"> POST /api/orders  cliente={customer} producto={product} cantidad={qty}");
    Console.WriteLine($"  HTTP {(int)response.StatusCode} {response.StatusCode}");
    Console.WriteLine($"  {Pretty(body)}");

    if (!response.IsSuccessStatusCode) return null;
    return JsonSerializer.Deserialize<OrderWithReceipt>(body, jsonOptions);
}

async Task ConfirmarAsync(HttpClient http, string baseUrl, string orderId)
{
    var response = await http.PostAsync($"{baseUrl}/api/orders/{orderId}/confirm", null);
    var body = await response.Content.ReadAsStringAsync();
    Console.WriteLine($"> POST /api/orders/{orderId}/confirm");
    Console.WriteLine($"  HTTP {(int)response.StatusCode} {response.StatusCode}");
    Console.WriteLine($"  {Pretty(body)}");
}

async Task EnviarAsync(HttpClient http, string baseUrl, string orderId)
{
    var response = await http.PostAsync($"{baseUrl}/api/orders/{orderId}/ship", null);
    var body = await response.Content.ReadAsStringAsync();
    Console.WriteLine($"> POST /api/orders/{orderId}/ship");
    Console.WriteLine($"  HTTP {(int)response.StatusCode} {response.StatusCode}");
    Console.WriteLine($"  {Pretty(body)}");
}

async Task<string?> GetJsonAsync(HttpClient http, string url)
{
    try
    {
        var response = await http.GetAsync(url);
        return await response.Content.ReadAsStringAsync();
    }
    catch (Exception ex)
    {
        return $"(no se pudo consultar {url}: {ex.Message})";
    }
}

async Task WaitForServiceAsync(string url, string name)
{
    for (var attempt = 1; attempt <= 30; attempt++)
    {
        try
        {
            var response = await http.GetAsync(url);
            if (response.IsSuccessStatusCode) return;
        }
        catch
        {
            // todavia no esta
        }

        if (attempt == 1) Console.WriteLine($"Esperando a {name}...");
        await Task.Delay(1000);
    }
}

static string Pretty(string? json)
{
    if (string.IsNullOrWhiteSpace(json)) return "";
    try
    {
        return JsonSerializer.Serialize(JsonSerializer.Deserialize<JsonElement>(json), new JsonSerializerOptions
        {
            WriteIndented = true
        });
    }
    catch
    {
        return json;
    }
}

static string Indent(string text) => string.Join(Environment.NewLine, text
    .Split(Environment.NewLine)
    .Select(line => "  " + line));