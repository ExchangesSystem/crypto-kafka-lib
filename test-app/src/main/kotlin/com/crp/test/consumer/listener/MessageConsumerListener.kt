package com.crp.test.consumer.listener

import com.crp.test.consumer.service.MessageProcessorService
import com.crp.test.dto.KafkaMessage
import com.crp.test.dto.UserEvent
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class MessageConsumerListener(
    @Autowired
    private val objectMapper: ObjectMapper,
    @Autowired
    private val messageProcessorService: MessageProcessorService
) {
    
    private val logger = LoggerFactory.getLogger(MessageConsumerListener::class.java)
    
    // Basic listener for default topic
    @KafkaListener(
        topics = ["\${kafka.topics.default:test-topic}"],
        groupId = "\${kafka.consumer.group-id:test-consumer-group}"
    )
    fun consumeMessage(message: String) {
        logger.info("Received message from default topic: {}", message)
        
        try {
            val kafkaMessage = objectMapper.readValue(message, KafkaMessage::class.java)
            messageProcessorService.processMessage(kafkaMessage)
        } catch (e: Exception) {
            logger.error("Error processing message: {}", e.message)
            // In production, you might want to send this to a DLQ (Dead Letter Queue)
            messageProcessorService.handleError(message, e)
        }
    }
    
    // Listener with headers and metadata
    @KafkaListener(
        topics = ["\${kafka.topics.metadata:metadata-topic}"],
        groupId = "\${kafka.consumer.group-id:test-consumer-group}"
    )
    fun consumeWithMetadata(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long
    ) {
        logger.info("Received message from topic '{}', partition: {}, offset: {}, timestamp: {}", 
            topic, partition, offset, timestamp)
        logger.info("Message content: {}", message)
        
        try {
            val kafkaMessage = objectMapper.readValue(message, KafkaMessage::class.java)
            messageProcessorService.processMessageWithMetadata(
                kafkaMessage, 
                topic, 
                partition, 
                offset, 
                timestamp
            )
        } catch (e: Exception) {
            logger.error("Error processing message with metadata: {}", e.message)
        }
    }
    
    // Listener for user events
    @KafkaListener(
        topics = ["\${kafka.topics.user-events:user-events}"],
        groupId = "\${kafka.consumer.group-id:test-consumer-group}"
    )
    fun consumeUserEvent(message: String) {
        logger.info("Received user event: {}", message)
        
        try {
            val userEvent = objectMapper.readValue(message, UserEvent::class.java)
            messageProcessorService.processUserEvent(userEvent)
        } catch (e: Exception) {
            logger.error("Error processing user event: {}", e.message)
        }
    }
    
    // Listener with manual acknowledgment
    @KafkaListener(
        topics = ["\${kafka.topics.manual-ack:manual-ack-topic}"],
        groupId = "\${kafka.consumer.group-id:test-consumer-group}",
        containerFactory = "kafkaManualAckListenerContainerFactory"
    )
    fun consumeWithManualAck(
        message: String,
        acknowledgment: Acknowledgment
    ) {
        logger.info("Received message with manual ack: {}", message)
        
        try {
            val kafkaMessage = objectMapper.readValue(message, KafkaMessage::class.java)
            val processed = messageProcessorService.processMessageWithValidation(kafkaMessage)
            
            if (processed) {
                // Acknowledge only if processing was successful
                acknowledgment.acknowledge()
                logger.info("Message acknowledged successfully")
            } else {
                // Don't acknowledge - message will be redelivered
                logger.warn("Message not acknowledged, will be redelivered")
            }
        } catch (e: Exception) {
            logger.error("Error processing message with manual ack: {}", e.message)
            // Don't acknowledge on error
        }
    }
    
    // Batch listener
    @KafkaListener(
        topics = ["\${kafka.topics.batch:batch-topic}"],
        groupId = "\${kafka.consumer.group-id:test-consumer-group}",
        containerFactory = "kafkaBatchListenerContainerFactory"
    )
    fun consumeBatch(messages: List<String>) {
        logger.info("Received batch of {} messages", messages.size)
        
        val kafkaMessages = messages.mapNotNull { message ->
            try {
                objectMapper.readValue(message, KafkaMessage::class.java)
            } catch (e: Exception) {
                logger.error("Error parsing message in batch: {}", e.message)
                null
            }
        }
        
        messageProcessorService.processBatch(kafkaMessages)
    }
    
    // Listener with ConsumerRecord for full control
    @KafkaListener(
        topics = ["\${kafka.topics.advanced:advanced-topic}"],
        groupId = "\${kafka.consumer.group-id:test-consumer-group}"
    )
    fun consumeRecord(record: ConsumerRecord<String, String>) {
        logger.info("Received record: key={}, value={}, partition={}, offset={}", 
            record.key(), 
            record.value(), 
            record.partition(), 
            record.offset()
        )
        
        // Access headers if needed
        record.headers().forEach { header ->
            logger.info("Header: {}={}", header.key(), String(header.value()))
        }
        
        try {
            val kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage::class.java)
            messageProcessorService.processRecord(record.key(), kafkaMessage)
        } catch (e: Exception) {
            logger.error("Error processing record: {}", e.message)
        }
    }
}