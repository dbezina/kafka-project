package com.bezina.ws.email_ntfctn.handler;

import com.bezina.ws.core.ProductCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics="product-created-events-topic")
public class ProductCreatedEventHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductCreatedEventHandler.class);

    @KafkaHandler
    public void handle(ProductCreatedEvent productCreatedEvent){
        System.out.println(productCreatedEvent.getProductId());
    }
}
