# Kafka Producer

POC de un servicio Spring Boot que expone una API REST para publicar eventos en Kafka.

## Stack

- Java 21
- Spring Boot 4.1.1 (Web, Kafka, Validation)
- Lombok
- Maven

## Cómo ejecutar

```bash
./mvnw spring-boot:run
```

## Cómo probar

```bash
./mvnw test
```

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
