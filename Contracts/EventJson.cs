using System.Text.Json;
using System.Text.Json.Serialization;

namespace Contracts;

/// <summary>
/// Opciones de serializacion compartidas por productor y consumidores, para que el
/// JSON que se publica y el que se deserializa sean exactamente compatibles.
/// </summary>
public static class EventJson
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };
}