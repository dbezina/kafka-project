package com.bezina.ws.email_ntfctn.handler;

import com.bezina.ws.core.ProductCreatedEvent;
import com.bezina.ws.email_ntfctn.error.NotRetryableException;
import com.bezina.ws.email_ntfctn.error.RetryableException;
import com.bezina.ws.email_ntfctn.io.ProcessEventRepository;
import com.bezina.ws.email_ntfctn.io.ProcessedEventEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Component
//@KafkaListener(topics="product-created-events-topic")
@KafkaListener(topics="product-created-events-topic" , groupId = "product-created-events")
public class ProductCreatedEventHandler {
    private final Logger LOGGER = LoggerFactory.getLogger(ProductCreatedEventHandler.class);
    private RestTemplate restTemplate;
    private ProcessEventRepository processEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate, ProcessEventRepository eventRepository) {

        this.restTemplate = restTemplate;
        this.processEventRepository = eventRepository;
    }
    //@Transactional
    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent productCreatedEvent,
                       @Header(value = "messageId", required = false) String messageId,
                       @Header(KafkaHeaders.RECEIVED_KEY) String messageKey){
        LOGGER.info("Received a new event " + productCreatedEvent.getTitle()+ " "+ productCreatedEvent.getProductId() );

        //Check if the message was already processed before
        ProcessedEventEntity existingRecord = processEventRepository.findByMessageId(messageId);
        if (existingRecord != null) {
            LOGGER.info("found a duplicate messageId");
            return;
        }

        String requestUrl = "http://localhost:8082/response/200";

        try {
            ResponseEntity<String> response = restTemplate.exchange(requestUrl, HttpMethod.GET, null, String.class);

            if (response.getStatusCode().value() == HttpStatus.OK.value()) {
                LOGGER.info("Received response from a remote service: " + response.getBody());
            }
        } catch (ResourceAccessException ex) {
            LOGGER.error(ex.getMessage());
            throw new RetryableException(ex);
        } catch(HttpServerErrorException ex) {
            LOGGER.error(ex.getMessage());
            throw new NotRetryableException(ex);
        } catch(Exception ex) {
            LOGGER.error(ex.getMessage());
            throw new NotRetryableException(ex);
        }

        // Save a unique message id in a database table
        try {
            processEventRepository.save(new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));
        } catch (DataIntegrityViolationException ex) {
            throw new NotRetryableException(ex.getMessage());

        }
    }

}
