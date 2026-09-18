# Kafka Producer

POC de un servicio Spring Boot que expone una API REST para publicar eventos en Kafka.

## Stack

- Java 21
- Spring Boot 4.1.1 (Web, Kafka, Validation)
- Lombok
- Maven

## Requisitos

- Un broker de Kafka accesible en `localhost:9092` (configurado en `application.yml`).

## Cómo ejecutar

```bash
./mvnw spring-boot:run
```

## Cómo probar

```bash
./mvnw test
```

## Configuración de Kafka

Definida en `src/main/resources/application.yml`:

- **Topic:** `inventory-events` (se crea automáticamente al levantar la app: 3 particiones, 1 réplica).
- **Producer:** `bootstrap-servers: localhost:9092`, con `StringSerializer` para key y value.

## Endpoints

### `POST /v1/inventory/event`

Recibe un evento de compra.

**Body de ejemplo:**

```json
{
  "productId": "P123",
  "orderId": "O456",
  "quantity": 2
}
```

**Respuesta:** el mismo evento recibido.

Adicionalmente, el evento se publica de forma asíncrona en el topic `inventory-events` usando el `orderId` como key del mensaje.
