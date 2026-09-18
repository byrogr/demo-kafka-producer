package com.rmsolutions.kafka_producer.controller;

import com.rmsolutions.kafka_producer.model.dto.EventDTO;
import com.rmsolutions.kafka_producer.producer.EventProducer;
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
public class EventController {

    private final EventProducer inventoryEventsProducer;

    @PostMapping("/event")
    public ResponseEntity<EventDTO> createPurchaseEvent(@RequestBody EventDTO eventDTO) {
        log.info("** Received purchase event: {} **", eventDTO);

        inventoryEventsProducer.sendInventoryEvent(eventDTO);

        return ResponseEntity.ok(eventDTO);
    }
}
