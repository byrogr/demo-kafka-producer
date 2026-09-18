package com.rmsolutions.kafka_producer.controller;

import com.rmsolutions.kafka_producer.model.dto.PurchaseEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/v1/inventory")
@RestController
@RequiredArgsConstructor
@Slf4j
public class InventoryEventsController {

    @PostMapping("/event")
    public ResponseEntity<PurchaseEvent> createPurchaseEvent(@RequestBody PurchaseEvent purchaseEvent) {
        log.info("** Received purchase event: {} **", purchaseEvent);

        // todo: invoke the kafka producer service

        return ResponseEntity.ok(purchaseEvent);
    }
}
