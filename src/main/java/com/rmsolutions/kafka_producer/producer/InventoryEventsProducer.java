package com.rmsolutions.kafka_producer.producer;

import com.rmsolutions.kafka_producer.model.dto.PurchaseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
            var completeStage = kafkaTemplate.send(topicName, key, value);
            completeStage.whenComplete((result, ex) -> handleSendResult(key, value, result, ex));
            log.info("** Purchase event sent to Kafka topic: {} **", value);
        } catch (Exception e) {
            log.error("** Error sending purchase event to Kafka: {} **", e.getMessage());
        }
    }

    private void handleSendResult(String key, String value, SendResult<String, String> result, Throwable ex) {
        if (ex != null) {
            log.error("** Error sending message with key {} and value {}: {} **", key, value, ex.getMessage());
        } else {
            log.info("** Message sent successfully with key {} and value {} in partition {} **",
                    key,
                    value,
                    result.getRecordMetadata().partition()
            );
        }
    }

}
