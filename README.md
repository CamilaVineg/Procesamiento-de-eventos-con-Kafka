# Ejercicio 3 — Procesamiento de eventos con Kafka

**Sistemas Distribuidos — UADER.** Práctica de comunicación por paso de mensajes.

---

## 1. Qué pide la consigna

> **Ejercicio 3 – Procesamiento de eventos con Kafka**
> Extender el sistema para que cada cambio de estado del pedido (creado, confirmado, enviado) se publique en un topic de Kafka y pueda ser consumido tanto por NotificationService como por un nuevo AnalyticsService.
> - Tecnologías: Kafka + Docker Compose
> - Pregunta: ¿qué diferencias notarías entre usar Kafka y RabbitMQ en este escenario?

Lo que hay que demostrar, entonces, son tres cosas:

1. **Un productor** que publica *cada cambio de estado* del pedido.
2. **Varios consumidores** del mismo topic, cada uno con su propio propósito (notificaciones y analítica).
3. Las **propiedades que hacen distinto a Kafka**: particionado, orden por clave, grupos de consumidores, retención y replay del log.

---

## 2. Arquitectura

```
                    OrdersService (8080)                    ┌──────────────────────────────┐
   cliente ───────►  POST /api/orders         ┌─────────────►│  TOPIC order-events          │
   HTTP            POST /api/orders/{id}/    │  producer    │  3 particiones, retenc. 7d  │
                   confirm                    │  idempotente │  key = orderId             │
                   POST /api/orders/{id}/ship │  acks=All    └───────────┬──────────────────┘
                                              │                          │
                        responde 202 Accepted │           ┌──────────────┴───────────────┐
                        (no espera a nadie)   │           │                              │
                                              │  ┌────────▼──────────┐   ┌───────────────▼──┐
                                              │  │ group             │   │ group             │
                                              │  │ notification-     │   │ analytics-        │
                                              │  │ service           │   │ service           │
                                              │  └────────┬──────────┘   └────────┬──────────┘
                                              │           │                       │
                                   SMTP (Mailpit)     REST /analytics/summary
                                   :1025 → :8025      :8082
```

**Lo importante del diagrama:** `OrdersService` no llama por HTTP a nadie. Publica el hecho ("el pedido ORD-X pasó a Confirmado") y se desvincula. Cada consumidor decide qué hacer con ese hecho, y puede hacerlo (o no hacerlo) sin volver a tocar el productor.

Por qué el topic tiene **3 particiones** y por qué el `key` es el `orderId`: Kafka garantiza orden **dentro** de una partición. Como las transiciones de un mismo pedido siempre viajan con la misma clave, siempre caen en la misma partición y el consumidor las lee en orden: nunca puede ver un `order.shipped` antes del `order.created` de ese pedido.

| | Valor | Por qué |
|---|---|---|
| Topic | `order-events` | Un solo topic de eventos del dominio |
| Particiones | `3` | Permite paralelismo y crece sin migrar datos |
| Réplicas | `1` (un solo broker) | Es una máquina de la facultad; en producción serían `3` brokers y factor `3` |
| Retención | 7 días (`retention.ms=604800000`) | El log se puede releer: eso habilita el replay |
| Clave | `orderId` | Orden garantizado por pedido |

---

## 3. Estructura del proyecto

```
Ejercicio 3 - KAFKA/
├── KafkaExercise.sln
├── docker-compose.yml              # Kafka (KRaft), Mailpit, 3 servicios + cliente de demo
├── Contracts/                      # contrato del evento, compartido por productor y consumidores
│   ├── OrderEvent.cs               #   evento + tipos + estados
│   ├── ApiContracts.cs             #   DTOs de la API de OrdersService
│   └── EventJson.cs                #   opciones de serialización compartidas
├── OrdersService/                  # PRODUCER: publica order.created / confirmed / shipped
│   ├── Program.cs                  #   endpoints + máquina de estados del pedido
│   ├── Services/OrderEventPublisher.cs   # producer idempotente, acks=All, key=orderId
│   ├── Services/OrderStore.cs      #   catálogo + stock + pedidos (en memoria)
│   ├── Services/KafkaOptions.cs
│   ├── Dockerfile
│   └── appsettings.json
├── NotificationService/            # CONSUMIDOR 1 (grupo notification-service)
│   ├── Program.cs                  #   /health, /notifications, /dead-letters
│   ├── Services/NotificationWorker.cs    # consume, envía el mail, commit manual, retry + DLQ
│   ├── Services/NotificationStore.cs     # registro + idempotencia (JSON Lines)
│   ├── Services/Options.cs
│   ├── Dockerfile
│   └── appsettings.json
├── AnalyticsService/               # CONSUMIDOR 2 (grupo analytics-service) → el nuevo de la consigna
│   ├── Program.cs                  #   /analytics/summary, /products, /customers, /recent, /consumer
│   ├── Services/AnalyticsWorker.cs       # consume + estado del consumidor (offsets y lag)
│   ├── Services/MetricsAggregator.cs     # métricas agregadas
│   ├── Services/Options.cs
│   ├── Dockerfile
│   └── appsettings.json
├── DemoClient/                     # consola que dispara los 4 escenarios de una vez
│   ├── Program.cs
│   └── Dockerfile
├── .dockerignore
├── .gitignore
└── README.md
```

---

## 4. Requisitos

| Herramienta | Para qué | ¿Obligatoria? |
|---|---|---|
| **Docker Desktop** | Levantar Kafka y los 3 servicios | Sí |
| **.NET 8 SDK** | Solo para compilar/depurar localmente | No (el SDK corre dentro de las imágenes) |

Instalación del SDK, si lo necesitás:

```powershell
winget install --id Microsoft.DotNet.SDK.8 -e
```

---

## 5. Cómo ejecutar

```powershell
docker compose up -d --build
```

| Servicio | Puerto en Windows | URL |
|---|---|---|
| OrdersService | 8080 | http://localhost:8080 |
| NotificationService | 8081 | http://localhost:8081 |
| AnalyticsService | 8082 | http://localhost:8082 |
| Kafka (broker) | 9092 | `localhost:9092` |
| Mailpit (buzón de los correos) | 8025 | http://localhost:8025 |

Escenario completo de la consigna en un comando:

```powershell
docker compose run --rm democlient
```

Para detener todo:

```powershell
docker compose down          # conserva los datos
docker compose down -v       # borra también el log de Kafka y los correos (estado limpio)
```

> **PowerShell:** usá `curl.exe` (con el `.exe`) en las pruebas, porque `curl` es alias de `Invoke-WebRequest` y no acepta `-d` ni `-X POST`.

---

## 6. Endpoints

### OrdersService (8080)

| Método | Ruta | Devuelve |
|---|---|---|
| `POST` | `/api/orders` | `202` pedido creado + evento `order.created` · `400` datos inválidos · `404` producto inexistente · `503` Kafka caído |
| `POST` | `/api/orders/{orderId}/confirm` | `202` + evento `order.confirmed` · `404` · `409` transición inválida o stock insuficiente (+ evento `order.rejected`) · `503` |
| `POST` | `/api/orders/{orderId}/ship` | `202` + evento `order.shipped` · `404` · `409` si el pedido no está confirmado · `503` |
| `GET` | `/api/orders` | `200` listado de pedidos |
| `GET` | `/api/orders/{orderId}` | `200` pedido · `404` |
| `GET` | `/api/catalog` | `200` catálogo con stock |
| `GET` | `/api/events` | `200` historial de eventos publicados por este servicio (topic, partición, offset) |
| `GET` | `/health` | `200` estado del productor |

Body del `POST /api/orders`:

```json
{ "customerId": "CLI-001", "customerEmail": "cli-001@tp11.local", "productId": "P001", "quantity": 3 }
```

La respuesta **trae el acuse de Kafka**: en qué `topic`, `partition` y `offset` quedó persistido el evento. Eso es la prueba de que el evento está en el log.

```json
{
  "order": { "orderId": "ORD-B200DF00", "productId": "P001", "quantity": 3, "total": 255000,
             "status": "Creado", "sequence": 1, "stateHistory": ["Creado"] },
  "event": { "eventId": "342c06e268bc47d0a0fea4d85705c34c", "eventType": "order.created",
             "topic": "order-events", "partition": 2, "offset": 0, "key": "ORD-B200DF00" }
}
```

### NotificationService (8081)

| Método | Ruta | Devuelve |
|---|---|---|
| `GET` | `/health` | `200` estado del consumidor, SMTP, `notificationsSent`, `deadLetters` |
| `GET` | `/notifications?take=N` | `200` correos enviados (últimos N) |
| `GET` | `/dead-letters?take=N` | `200` eventos que agotaron los reintentos |

### AnalyticsService (8082)

| Método | Ruta | Devuelve |
|---|---|---|
| `GET` | `/health` | `200` |
| `GET` | `/analytics/summary` | `200` métricas agregadas del topic |
| `GET` | `/analytics/products?take=N` | `200` unidades e ingresos por producto |
| `GET` | `/analytics/customers?take=N` | `200` pedidos por cliente |
| `GET` | `/analytics/recent?take=N` | `200` últimos eventos recibidos |
| `GET` | `/analytics/consumer` | `200` grupo, particiones asignadas, offsets confirmados y **lag** |

### El evento (`Contracts/OrderEvent.cs`)

```json
{
  "eventId": "342c06e268bc47d0a0fea4d85705c34c",
  "eventType": "order.created",
  "eventVersion": 1,
  "occurredAt": "2026-10-18T02:51:40.7565823+00:00",
  "producer": "orders-service",
  "orderId": "ORD-B200DF00",
  "customerId": "CLI-001",
  "customerEmail": "cli-001@tp11.local",
  "productId": "P001",
  "quantity": 3,
  "unitPrice": 85000,
  "total": 255000,
  "currency": "ARS",
  "status": "Creado",
  "sequence": 1,
  "reason": null
}
```

`eventId` es la clave de la **idempotencia** del consumidor; `sequence` es el número de cambio dentro del ciclo de vida del pedido.

---

## 7. Formas de probar las funcionalidades

### Prueba 1 — Un cambio de estado se publica y lo reciben los DOS consumidores

```powershell
curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-001\",\"productId\":\"P001\",\"quantity\":3}'
```

```json
{"order":{"orderId":"ORD-B200DF00", ... ,"status":"Creado","sequence":1},
 "event":{"eventType":"order.created","topic":"order-events","partition":2,"offset":0,"key":"ORD-B200DF00"}}   →  HTTP 202
```

Apenas un segundo después:

```powershell
curl.exe http://localhost:8081/notifications?take=1     # consumidor 1: ya mandó el correo
curl.exe http://localhost:8082/analytics/recent?take=1   # consumidor 2: ya lo tiene en sus métricas
```

```json
{"total":1,"items":[{"eventType":"order.created","orderId":"ORD-B200DF00",
 "to":"cli-001@tp11.local","subject":"Pedido ORD-B200DF00 recibido",
 "sourceTopic":"order-events","sourcePartition":2,"sourceOffset":0,"attempt":1}]}
```

**Qué demuestra:** los dos consumidores tienen **grupos distintos** (`notification-service` y `analytics-service`), y por eso ambos reciben el 100 % de los eventos del mismo topic. Es el comportamiento *fan-out* de Kafka: un productor, muchos consumidores independientes.

### Prueba 2 — El ciclo de vida completo: creado → confirmado → enviado

```powershell
curl.exe -X POST http://localhost:8080/api/orders/ORD-B200DF00/confirm
curl.exe -X POST http://localhost:8080/api/orders/ORD-B200DF00/ship
```

```json
{"order":{"status":"Confirmado","sequence":2,"stateHistory":["Creado","Confirmado"]},
 "event":{"eventType":"order.confirmed","partition":2,"offset":1,"key":"ORD-B200DF00"}}   →  HTTP 202

{"order":{"status":"Enviado","sequence":3,"stateHistory":["Creado","Confirmado","Enviado"]},
 "event":{"eventType":"order.shipped","partition":2,"offset":2,"key":"ORD-B200DF00"}}     →  HTTP 202
```

Mirá el detalle: **los tres eventos del mismo pedido están en la partición 2 y con offsets correlativos (0, 1, 2)**. Eso es la clave `orderId` funcionando:Kafka mantiene el orden por clave.

En Mailpit (http://localhost:8025) hay tres correos del mismo pedido: "recibido", "confirmado" y "Tu pedido está en camino".

### Prueba 3 — Transición inválida → 409 (no se publica ningún evento)

```powershell
curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-004\",\"productId\":\"P001\",\"quantity\":1}'
curl.exe -X POST http://localhost:8080/api/orders/ORD-XXXX/ship     # sin confirmar
```

```json
{"message":"Transicion invalida: el pedido esta en estado Creado. Solo se envia un pedido confirmado.","orderId":"ORD-XXXX"}   →  HTTP 409
```

**Qué demuestra:** la máquina de estados se valida en el productor y **el topic no se toca**: el log solo refleja hechos que ocurrieron.

### Prueba 4 — Stock insuficiente → el rechazo también es un evento

```powershell
curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-002\",\"productId\":\"P002\",\"quantity\":999}'
curl.exe -X POST http://localhost:8080/api/orders/ORD-53FB1786/confirm
```

```json
{"message":"Stock insuficiente. Disponible: 50, solicitado: 999.",
 "orderId":"ORD-53FB1786",
 "event":{"eventType":"order.rejected","topic":"order-events","partition":2,"offset":3}}   →  HTTP 409
```

Y `AnalyticsService` ya lo cuenta:

```json
"orders": { "created": 4, "confirmed": 2, "shipped": 2, "rejected": 1 }
```

**Qué demuestra:** los eventos no son solo "cosas que salieron bien". Un consumidor de analítica tiene que ver también los rechazos para que los números cierren.

### Prueba 5 — Las métricas que calculó el segundo consumidor

```powershell
curl.exe http://localhost:8082/analytics/summary
```

```json
{
  "totalEvents": 9, "duplicatesIgnored": 0, "malformedEvents": 0,
  "orders": { "created": 4, "confirmed": 2, "shipped": 2, "rejected": 1,
              "distinctCustomers": 4, "distinctOrders": 4 },
  "revenue": { "currency": "ARS", "confirmed": 1805000, "shipped": 1805000,
               "confirmedUnits": 8, "averageTicket": 902500 },
  "eventsByType": { "order.created": 4, "order.confirmed": 2, "order.shipped": 2, "order.rejected": 1 },
  "unitsByProduct": { "P001": 3, "P003": 5 },
  "revenueByProduct": { "P001": 255000, "P003": 1550000 },
  "ordersByCustomer": { "CLI-001": 1, "CLI-002": 1, "CLI-003": 1, "CLI-004": 1 },
  "ordersInFlight": 1,
  "lastStatusByOrder": { "ORD-B8DBBA77": "Enviado", "ORD-3460AF1C": "Rechazado",
                         "ORD-1EACC2DF": "Enviado", "ORD-C78ED380": "Creado" }
}
```

**Qué demuestra:** `AnalyticsService` es un consumidor **nuevo**, escrito sin tocar una línea de `OrdersService` ni de `NotificationService`. Ese es el punto del procesamiento de eventos: agregar un consumidor no obliga a modificar al productor.

### Prueba 6 — Estado del consumidor: offsets y lag por partición

```powershell
curl.exe http://localhost:8082/analytics/consumer
```

```json
{
  "consumer": { "groupId": "analytics-service", "topic": "order-events", "state": "running",
                "assignedPartitions": [0,1,2], "processedUpTo": { "0": 6, "1": 5, "2": 2 } },
  "partitions": [ { "partition": 0, "committedOffset": 6, "logEndOffset": 6, "lag": 0 },
                  { "partition": 1, "committedOffset": 5, "logEndOffset": 5, "lag": 0 },
                  { "partition": 2, "committedOffset": 2, "logEndOffset": 2, "lag": 0 } ]
}
```

Y desde la CLI de Kafka:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group notification-service
```

```
GROUP                TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
notification-service  order-events        0          6               6               0    notification-service-f530bf1b-…
notification-service  order-events        1          5               5               0    notification-service-f530bf1b-…
notification-service  order-events        2          2               2               0    notification-service-f530bf1b-…
```

**Qué demuestra:** el offset es la unidad de progreso del consumidor (el equivalente conceptual del *ack* de RabbitMQ), y el **lag** es la distancia entre lo último que hay en el log y lo último que consumió el grupo. Es la métrica que se monitorea en producción.

### Prueba 7 — Ver el topic crudo (el "log")

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:29092 `
  --topic order-events --from-beginning --max-messages 5
```

```json
{"eventId":"342c06e2…","eventType":"order.created",…,"orderId":"ORD-B200DF00","status":"Creado","sequence":1}
{"eventId":"ede60d93…","eventType":"order.confirmed",…,"orderId":"ORD-B200DF00","status":"Confirmado","sequence":2}
{"eventId":"27ec53ef…","eventType":"order.shipped",…,"orderId":"ORD-B200DF00","status":"Enviado","sequence":3}
```

También se puede ver la distribución:

```powershell
docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic order-events
docker compose exec kafka kafka-get-offsets --bootstrap-server localhost:29092 --topic order-events
```

**Qué demuestra:** el topic no es una cola: **el mensaje sigue ahí después de que los consumidores lo leyeron**. Con un grupo nuevo (prueba 9) se vuelve a leer el mismo mensaje.

### Prueba 8 — NotificationService caído: los eventos quedan en el log

```powershell
docker compose stop notification

curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-CAIDO\",\"productId\":\"P001\",\"quantity\":2}'
curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-CAIDO\",\"productId\":\"P001\",\"quantity\":1}'
```

Mientras está caído, el productor **sigue funcionando**: no hay coupling. Y el grupo accumulates atraso:

```powershell
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group notification-service
```

Ahora se lo levanta:

```powershell
docker compose start notification
Start-Sleep -Seconds 8
curl.exe http://localhost:8081/notifications?take=2
```

```json
{"total":2,"items":[{"eventType":"order.created","orderId":"ORD-EE2FEB01","to":"cli-caido@tp11.local",
  "subject":"Pedido ORD-EE2FEB01 recibido","sourcePartition":2,"sourceOffset":1}]}
```

**Qué demuestra:** nadie perdió nada. El log retiene los eventos y el consumidor los procesa al volver, desde el offset exacto donde se había quedado. (En RabbitMQ el comportamiento es equivalente si la cola y los mensajes son durables: la diferencia real es *qué se puede hacer después* con esos datos — ver sección 10.)

### Prueba 9 — Replay: releer el histórico cambiando el `GroupId`

Las métricas de `AnalyticsService` viven en memoria, así que si se reinicia el contenedor quedan en cero (aunque los offsets del grupo siguen donde estaban). Para **reconstruirlas desde todo el log**, se levanta la misma imagen con un grupo nuevo:

```powershell
docker compose run -d --name tp11-replay -e Kafka__GroupId=analytics-replay -p 8083:8080 analytics
Start-Sleep -Seconds 15
curl.exe http://localhost:8083/analytics/summary
docker rm -f tp11-replay
```

```json
{"totalEvents":12, "orders": {"created":7,"confirmed":2,"shipped":2,"rejected":1},
 "revenue": {"confirmed":1805000, "confirmedUnits":8},
 "ordersByCustomer": {"CLI-001":1,"CLI-002":1,"CLI-003":1,"CLI-004":1,"CLI-CAIDO":2,"CLI-DLQ":1}}
```

Y en el log:

```
AnalyticsService consumiendo topic=order-events group=analytics-replay (modo Earliest)
Asignadas particiones de order-events: 0, 1, 2
```

**Qué demuestra:** en Kafka el "grupo de consumidores" es el estado del consumidor, no una identidad del topic. Un `GroupId` nuevo + `auto.offset.reset=earliest` reprocesa el histórico completo. Con una cola de RabbitMQ esto no existe: el mensaje se consume una vez y desaparece.

### Prueba 10 — Escalar un consumidor (reparto de particiones)

Dos instancias del **mismo grupo** se reparten el trabajo:

```powershell
docker compose run -d --name tp11-analytics-2 -e Kafka__GroupId=analytics-service analytics
Start-Sleep -Seconds 15
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group analytics-service
```

```
GROUP              TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  HOST
analytics-service  order-events   0          5               5               0    /172.22.0.6
analytics-service  order-events   1          5               5               0    /172.22.0.6
analytics-service  order-events   2          2               2               0    /172.22.0.7   ← la 2da instancia
```

```powershell
docker rm -f tp11-analytics-2
```

**Qué demuestra:** el escalado horizontal no requiere cambiar el producer ni la configuración del topic: se agrega una instancia y Kafka le reparte particiones. **El límite del paralelismo es la cantidad de particiones** (3 en este ejercicio). Y como consecuencia directa: dentro de un mismo grupo, los agregados quedan repartidos entre instancias (por eso `AnalyticsService` está pensado para una sola instancia, o con un store compartido).

### Prueba 11 — Reintentos y DLQ

`NotificationService` falla a propósito con la variable `FAIL_MODE`:

```powershell
$env:FAIL_MODE = 'always'
docker compose up -d --force-recreate notification
Start-Sleep -Seconds 6

curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-DLQ\",\"customerEmail\":\"dlq@tp11.local\",\"productId\":\"P002\",\"quantity\":1}'
Start-Sleep -Seconds 15

curl.exe http://localhost:8081/dead-letters
```

```json
{"total":1,"items":[{
  "eventType":"order.created","orderId":"ORD-2C8AE535","attempts":3,
  "error":"SMTP no disponible (FAIL_MODE=always) al enviar a dlq@tp11.local",
  "sourceTopic":"order-events-retry","sourcePartition":0,"sourceOffset":1,
  "rawPayload":"{\"eventId\":\"a8c8500f…\",\"eventType\":\"order.created\",…}"}]}
```

En el log del servicio:

```
Fallo el envio de a8c8500f… (intento 1/3). Se republica en order-events-retry
Evento a8c8500f… republicado en order-events-retry (x-retry-count=1)
Fallo el envio de a8c8500f… (intento 2/3). Se republica en order-events-retry
Evento a8c8500f… republicado en order-events-retry (x-retry-count=2)
Evento a8c8500f… agoto 3 intentos -> DLQ order-events-dlq
```

Volver a la normalidad:

```powershell
Remove-Item Env:\FAIL_MODE
docker compose up -d --force-recreate notification
```

**Qué demuestra:** Kafka **no tiene DLQ nativas**: hay que armarla con topics. El patrón es `retry topic` (con un header `x-retry-count`) + `DLQ topic`, y se vuelve a leer de la DLQ con:

```powershell
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:29092 `
  --topic order-events-dlq --from-beginning
```

El modo `transient2` (falla los 2 primeros intentos de cada evento y después funciona) sirve para ver el reintento *exitoso*:

```powershell
$env:FAIL_MODE = 'transient2'; docker compose up -d --force-recreate notification
```

### Prueba 12 — Kafka caído: el productor no miente

```powershell
docker compose stop kafka
Start-Sleep -Seconds 3

curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-SINKAFKA\",\"productId\":\"P001\",\"quantity\":1}'
```

```json
{"message":"Kafka no esta disponible: el pedido no se registro. Reintentar.","orderId":"","event":null}   →  HTTP 503
```

Al levantar Kafka, todo sigue funcionando sin reiniciar los servicios:

```powershell
docker compose start kafka
Start-Sleep -Seconds 25
curl.exe -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{\"customerId\":\"CLI-OK\",\"productId\":\"P001\",\"quantity\":1}'
```

```json
{"order":{"orderId":"ORD-E1751F9B","status":"Creado"},
 "event":{"eventType":"order.created","partition":0,"offset":5}}   →  HTTP 202
```

**Qué demuestra:** el productor es **idempotente y con `acks=All`**: no devuelve "hecho" hasta que el broker confirmó la escritura. Con `Kafka` caído, la API responde `503` y **el pedido no se registra**, así que no existen eventos perdidos. Esa es la garantía que hay que elegir explícitamente (se podría elegir lo contrario: fire-and-forget y aceptar la pérdida).

### Prueba 13 — Idempotencia del consumidor

El registro de notificaciones se persiste en JSON Lines dentro del contenedor:

```powershell
docker compose exec notification sh -c "cat /app/data/notifications.jsonl"
```

```json
{"eventId":"342c06e2…","eventType":"order.created","orderId":"ORD-B200DF00","to":"cli-001@tp11.local","sentAt":"…"}
```

`NotificationService` marca el `eventId` **después** de enviar el correo, así que si el proceso muere entre el envío y el commit del offset, al volver a recibir el evento lo descarta:

```
Duplicado descartado (idempotencia): order.created ORD-B200DF00 eventId=342c06e2…
```

```powershell
curl.exe http://localhost:8081/health
```

```json
{"notificationsSent": 3, "consumer": { "failMode": "none", "maxAttempts": 3 }}
```

**Qué demuestra:** Kafka entrega **al menos una vez** (at-least-once), no exactamente una. El consumidor tiene que ser idempotente — y en este caso eso significa "no mandar dos veces el mismo correo".

---

## 8. Cómo cumple la consigna

| Requisito de la consigna | Dónde está implementado | Cómo se comprueba |
|---|---|---|
| **Cada cambio de estado** (creado, confirmado, enviado) se publica en un topic | `OrdersService/Services/OrderEventPublisher.cs` + `POST /api/orders`, `/confirm`, `/ship` | Pruebas 1, 2, 3, 4 y 7 |
| El topic se llama `order-events` y tiene 3 particiones | `docker-compose.yml` → servicio `kafka-init` | `kafka-topics --describe --topic order-events` |
| Los eventos los consume **NotificationService** | `NotificationService/Services/NotificationWorker.cs` (grupo `notification-service`) → correo SMTP | Pruebas 1, 2 y 13; Mailpit |
| Los eventos los consume un **nuevo AnalyticsService** | `AnalyticsService/*` (grupo `analytics-service`) → métricas REST | Pruebas 5 y 6 |
| **Varios consumidores del mismo topic** (fan-out) | Dos `GroupId` distintos leyendo `order-events` | Prueba 1 y 6 |
| **Tecnología: Kafka** | `Confluent.Kafka` 2.16 sobre broker Apache Kafka 3.9 (KRaft) | Sección 2 y `docker-compose.yml` |
| **Docker Compose** | `docker-compose.yml` + un `Dockerfile` por proyecto | `docker compose up -d --build` |
| **Pregunta: diferencias entre Kafka y RabbitMQ** | Sección 10 de este documento (y `Comparacion_Kafka_vs_RabbitMQ.docx`) | — |

---

## 9. Las cinco diferencias que se ven en este ejercicio

Estas son las propiedades de Kafka que el código usa y que en RabbitMQ no existen (o se resuelven distinto):

1. **El topic es un log, no una cola.** El mensaje sigue ahí después de ser leído. → Pruebas 7 y 9.
2. **El consumidor decide desde dónde leer** (su `GroupId` + offsets). Un grupo nuevo = reproceso total. → Prueba 9.
3. **Orden por clave, no orden global.** La clave `orderId` garantiza que las transiciones de un pedido lleguen en orden; las de pedidos distintos pueden intercalarse. → Prueba 2.
4. **El escalado reparte particiones automáticamente** y su límite es el número de particiones. → Prueba 10.
5. **La retención es una política del log**, no de la cola. Con 7 días de retención se puede reprocesar la semana pasada. → Prueba 9.

---

## 10. Comparación: Kafka frente a RabbitMQ en este escenario

### 10.1 Modelo mental

| | **RabbitMQ** (message broker / cola) | **Apache Kafka** (event streaming platform) |
|---|---|---|
| Unidad | **Cola**: un mensaje, un consumidor, y el mensaje se consume | **Log particionado**: un evento, muchos lectores, y el evento se conserva |
| Almacenamiento | Los mensajes se borran al ser confirmados | Se guardan según `retention.ms` / `retention.bytes` |
| Estado del consumidor | La cola sabe quién confirmó (ACK) y reintenta | El consumidor guarda su offset (en Kafka: topic `__consumer_offsets`) |
| Reprocesar histórico | Imposible: el mensaje ya no existe | Trivial: nuevo `GroupId` (o reset de offsets) |
| Enrutado | Exchange + routing key + bindings (topic/direct/fanout) | Todo entra por topics; el enrutado lo hace la clave y los grupos |

### 10.2 Aplicado a este ejercicio concreto

| Criterio | Con RabbitMQ (ejercicio 2) | Con Kafka (este ejercicio) | Qué se nota en la práctica |
|---|---|---|---|
| **Estado del pedido** | `Pedido Creado` (un mensaje) | `order.created`, `order.confirmed`, `order.shipped`, `order.rejected` (un topic de eventos) | Con Kafka el consumidor sabe *la historia completa* del pedido, no solo el último hecho |
| **Notificaciones y analítica** | Habría que duplicar mensajes en dos colas, o usar un exchange `fanout` | Los dos leen `order-events` con grupos distintos | Agregar `AnalyticsService` **no tocó** ni `OrdersService` ni `NotificationService` |
| **Caída de NotificationService** | El mensaje espera en la cola durable | El evento espera en el log, con su partición y su offset | **Idéntico en los dos.** Kafka no aporta una garantía acá que RabbitMQ no tenga |
| **Nueva analítica histórica** ("cuánto vendimos el mes pasado") | Imposible: los mensajes ya se consumieron | `GroupId` nuevo + `Earliest` y se reprocesa el histórico retenido | La diferencia más grande y concreta del ejercicio (prueba 9) |
| **Auditoría / reconstrucción de estado** | Hay que reconstruirla desde los mensajes que quedaron | El log es la fuente de verdad; el estado actual se puede derivar | `AnalyticsService` es, literalmente, una materialized view del log |
| **Recuperar un consumidor caído que ya cometió efectos** | ACK manual para no perder; redelivery puede duplicar el efecto | Commit manual del offset + clave de idempotencia (`eventId`) | Es la misma mecánica; Kafka solo estandariza el offset |
| **Reintentos y DLQ** | Nativos: `dead-letter-exchange`, reintentos con TTL | Hay que **armarlos a mano** con topics (`retry` + `dlq`) | Kafka es peor acá: más código (prueba 11) |
| **Orden** | El orden depende de la cola; 1 mensaje a la vez por consumidor | Orden **por clave** dentro de una partición | Con Kafka hay que elegir la clave bien (nosotros: `orderId`) |
| **Paralelismo** | N consumers de la misma cola reparten mensajes | N consumers del mismo grupo reparten **particiones** | El paralelismo máximo lo fija la cantidad de particiones (3 acá) |
| **Latencia** | Menor: entrega al consumidor, sin escribir a log primero | Mayor: el productor espera el ack del log (`acks=All`) + consumo posterior | Aceptable para eventos; sería una mala elección para un login |
| **Productor caído / broker caído** | Con *publisher confirms* tampoco se pierde nada | Ídem, con `acks=All` + productor idempotente | Sin confirms, **ambos** pueden perder mensajes: el problema no es de la tecnología |
| **Operational** | Un broker, management UI en 15672, más simple | Cluster (KRaft/ZK), hay que monitorear particiones, lag y retención | Kafka es un componente más pesado para este dominio |
| **Ecosistema** | Job queues, RPC,acks, prioridad, TTL por mensaje | Kafka Streams, ksqlDB, schema registry, Exactly-once, Connect | RabbitMQ para mensajes; Kafka para datos y analítica |

### 10.3 Conclusión

Para **este escenario** las dos tecnologías funcionan y resuelven el mismo problema de desacoplamiento. Si el consigna fuera "mandar el mail y listo", **RabbitMQ sería la elección más simple**: cola durable, DLQ nativa, management UI, menos piezas que monitorear.

Kafka shinea cuando el mismo evento **interesa a varios consumidores con intereses distintos** y, sobre todo, cuando uno de ellos necesita **leer el pasado**: guardar métricas históricas, reindexar, auditar, reprocesar. Acá es exactamente lo que hace `AnalyticsService`, y esa capacidad —leer el log retenido con un `GroupId` nuevo— es lo que no se puede lograr con una cola.

El error clásico es tratar a Kafka como "una cola más rápida": se paga el costo operativo de Kafka y no se usan sus ventajas. Y el error simétrico es usar Kafka para mandar un email de confirmación.

---

## 11. Limitaciones conocidas

- **Todo el estado está en memoria.** El catálogo y los pedidos viven en el proceso de `OrdersService`; reiniciar el contenedor resetea el stock. Los correos ya enviados sí quedan en `/app/data/notifications.jsonl` (volumen).
- **Publicar y guardar el pedido no es atómico.** Se publica primero y después se actualiza el estado local; si el proceso muere en el medio, el evento ya está en Kafka pero el pedido local quedó desactualizado. La solución real es el **patrón outbox** (guardar el evento en la misma transacción que el pedido y un *relay* lo publica) o una transacción de Kafka. Acá se compensa devolviendo el stock si falla la publicación.
- **Sin seguridad**: PLAINTEXT y sin autenticación. En producción, SASL/SSL y ACLs por topic.
- **Réplica única**: un solo broker significa que si se pierde el disco se pierde el log. En producción, `min.insync.replicas=2` y factor de replicación 3.
- **La analítica no persiste**: reiniciar `AnalyticsService` deja las métricas en cero (aunque los offsets del grupo siguen). Para recomputar hay que usar un `GroupId` nuevo (prueba 9); para producción, un store externo o Kafka Streams con *changelog*.
- **Exactly-once no está resuelto**: hay idempotencia en el productor y deduplicación por `eventId` en el consumidor, pero eso es "efectivamente una vez", no exactamente una. Kafka da Exactly-once real solo dentro de una transacciónKafka (read-process-write), que no aplica a un consumidor que habla SMTP.
- **El `retry topic` usa la espera en línea** (`RETRY_DELAY_MS`), no un delay real del broker: el consumidor duerme entre intentos.
- **Sin métricas exporters**: el estado del consumidor se expone por REST en vez de por Prometheus.

---

## 12. Comandos útiles

```powershell
docker compose up -d --build                # levantar todo
docker compose run --rm democlient           # escenario completo de la consigna
docker compose logs -f orders                # ver los eventos publicados
docker compose logs -f notification          # ver los correos enviados
docker compose ps -a                         # estado (incluye kafka-init, que termina con Exited 0)
docker compose stop notification             # simular caída del consumidor
docker compose start notification            # recuperarlo y procesar lo acumulado
docker compose stop kafka                    # simular caída del broker (la API responde 503)
docker compose start kafka
docker compose restart orders                # reinicia OrdersService (vuelve el stock a 100/50/5)
docker compose down                          # apagar
docker compose down -v                       # apagar y borrar el log de Kafka y los correos

# CLI de Kafka
docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --describe --topic order-events
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic order-events --from-beginning --max-messages 5
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --list
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group analytics-service

# Replay (grupo nuevo leyendo todo el log)
docker compose run -d --name tp11-replay -e Kafka__GroupId=analytics-replay -p 8083:8080 analytics
docker rm -f tp11-replay

# Compilar local (opcional)
dotnet build KafkaExercise.sln
```

---

## 13. Problemas frecuentes

| Síntoma | Causa | Solución |
|---|---|---|
| `failed to connect to the docker API at npipe://...` | Docker Desktop apagado | `Start-Process "$env:LOCALAPPDATA\Programs\DockerDesktop\Docker Desktop.exe"` |
| `Container tp11-... is already in use` | Quedó un stack anterior del ejercicio 2 corriendo con los mismos nombres | `docker compose down` en la carpeta del ejercicio 2, o `docker rm -f <nombre>` |
| `The container name "/tp11-mailpit" is already in use` | El Mailpit del ejercicio 2 usa ese nombre | Los contenedores de este ejercicio se llaman `tp11-kafka-*`; si persiste, `docker rm -f tp11-mailpit` |
| `kafka-consumer-groups.sh: executable file not found` | En la imagen Confluent los binarios no llevan `.sh` | Usar `kafka-consumer-groups`, `kafka-console-consumer`, `kafka-topics` |
| `curl : la opción '-d' no se reconoce` | En PowerShell `curl` es alias de `Invoke-WebRequest` | Usar `curl.exe` |
| El JSON del POST muestra `\"customerId\"` literal | PowerShell no escapa las comillas dentro de comillas simples | Usar exactamente el formato de este README (comillas simples afuera) o `--%` |
| `AnalyticsService` queda sin puerto (conexión rechazada) | El consumidor síncrono bloqueaba el arranque del host | Ya corregido con `await Task.Yield()`; si vuelve a pasar, revisar que el worker no tenga un loop bloqueante |
| `/analytics/summary` devuelve todo en cero | El contenedor de analítica se reinició y las métricas son en memoria | Normal: usá un `GroupId` nuevo para recomputar desde el log (prueba 9) |
| `403 Forbidden` al escribir en `localhost:9092` desde el host | El listener externo está declarado como `localhost:9092` y Kafka espera el nombre exacto | Usá siempre `localhost:9092`, no `127.0.0.1` |

**Si estás dentro de OneDrive:** OneDrive con *Files On-Demand* deja los archivos como placeholders que existen solo en la nube. Windows los lee sin problema, pero **Docker Desktop no**, y el build falla. Solución: click derecho en la carpeta → **Siempre disponible en este dispositivo**.
