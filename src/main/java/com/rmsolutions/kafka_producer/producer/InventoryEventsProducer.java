package com.rmsolutions.kafka_producer.producer;

import com.rmsolutions.kafka_producer.model.dto.PurchaseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@Slf4j
@RequiredArgsConstructor
public class InventoryEventsProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.topic.name}")
    public String topicName;

    public void sendInventoryEvent(PurchaseEvent purchaseEvent) {
        try {
            var key = purchaseEvent.getOrderId();
            var value = objectMapper.writeValueAsString(purchaseEvent);
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            var completableSendStage = kafkaTemplate.send(record);
            completableSendStage.whenComplete((result, ex) ->
                    handleSendResult(result.getProducerRecord(), result.getRecordMetadata().partition(), ex)
            );
            log.info("** Purchase event sent to Kafka topic: {} **", value);
        } catch (Exception e) {
            log.error("** Error sending purchase event to Kafka: {} **", e.getMessage());
        }
    }

    private void handleSendResult(ProducerRecord<String, String> record, int partition, Throwable ex) {
        String key = record.key(), value = record.value();
        var partitionInfo = record.partition() != null ? record.partition() : partition;
        if (ex != null) {
            log.error("** Error sending message with key {} and value {}: {} **", key, value, ex.getMessage());
        } else {
            log.info("** Message sent successfully with key {} and value {} in partition {} **",
                    key,
                    value,
                    partitionInfo
            );
        }
    }

}
