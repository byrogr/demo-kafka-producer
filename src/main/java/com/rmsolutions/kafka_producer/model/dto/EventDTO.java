package com.rmsolutions.kafka_producer.model.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventDTO {
    private String productId;
    private String orderId;
    private int quantity;
}
