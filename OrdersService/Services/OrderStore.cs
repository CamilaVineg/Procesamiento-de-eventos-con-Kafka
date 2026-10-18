using System.Collections.Concurrent;
using Contracts;

namespace OrdersService.Services;

public sealed record CatalogItem(string ProductId, string ProductName, decimal UnitPrice, int Stock);

/// <summary>
/// Estado del sistema: catalogo con stock + pedidos.
///
/// En el Ejercicio 1 el stock vivia en InventoryService y se consultaba por HTTP.
/// Acá se mantiene local a proposito: el foco del ejercicio 3 es que el pedido
/// se resuelve sin depender de que ningun otro servicio este en linea.
/// Todo esta en memoria, asi que reiniciar el contenedor resetea el stock.
/// </summary>
public sealed class OrderStore
{
    private readonly ConcurrentDictionary<string, Order> _orders = new();
    private readonly Dictionary<string, CatalogItem> _catalog;
    private readonly object _stockGate = new();

    public OrderStore(IConfiguration configuration)
    {
        _catalog = new Dictionary<string, CatalogItem>(StringComparer.OrdinalIgnoreCase)
        {
            ["P001"] = new("P001", "Teclado mecanico", 85_000m, 100),
            ["P002"] = new("P002", "Mouse inalambrico", 42_500m, 50),
            ["P003"] = new("P003", "Monitor 24\"", 310_000m, 5)
        };

        foreach (var item in _catalog.Values)
        {
            var overrideStock = configuration.GetValue<int?>($"Catalog:Stock:{item.ProductId}");
            if (overrideStock is not null) _catalog[item.ProductId] = item with { Stock = overrideStock.Value };
        }
    }

    public IReadOnlyList<ProductStockResponse> Catalog() => _catalog.Values
        .OrderBy(p => p.ProductId)
        .Select(p => new ProductStockResponse
        {
            ProductId = p.ProductId,
            ProductName = p.ProductName,
            UnitPrice = p.UnitPrice,
            Stock = p.Stock
        })
        .ToArray();

    public bool TryGetProduct(string productId, out CatalogItem item) =>
        _catalog.TryGetValue(productId, out item!);

    public int AvailableStock(string productId)
    {
        lock (_stockGate)
        {
            return _catalog.TryGetValue(productId, out var item) ? item.Stock : 0;
        }
    }

    /// <summary>Descuenta stock si alcanza. Es la operacion atomica que evita sobre-vender.</summary>
    public bool TryReserve(string productId, int quantity, out int remaining)
    {
        lock (_stockGate)
        {
            if (!_catalog.TryGetValue(productId, out var item) || item.Stock < quantity)
            {
                remaining = _catalog.TryGetValue(productId, out var current) ? current.Stock : 0;
                return false;
            }

            _catalog[productId] = item with { Stock = item.Stock - quantity };
            remaining = _catalog[productId].Stock;
            return true;
        }
    }

    /// <summary>Compensacion: devuelve stock si el evento no pudo publicarse.</summary>
    public void Release(string productId, int quantity)
    {
        lock (_stockGate)
        {
            if (_catalog.TryGetValue(productId, out var item))
                _catalog[productId] = item with { Stock = item.Stock + quantity };
        }
    }

    public void Add(Order order) => _orders[order.OrderId] = order;

    public bool TryGet(string orderId, out Order order) => _orders.TryGetValue(orderId, out order!);

    public IReadOnlyList<OrderResponse> All() => _orders.Values
        .OrderBy(o => o.CreatedAt)
        .Select(OrderResponse.From)
        .ToArray();

    public int Count => _orders.Count;
}