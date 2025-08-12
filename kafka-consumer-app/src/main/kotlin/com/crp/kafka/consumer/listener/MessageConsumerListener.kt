package com.crp.kafka.consumer.listener

import com.crp.kafka.consumer.service.MessageProcessorService
import com.crp.kafka.shared.constants.KafkaConstants
import com.crp.kafka.shared.dto.*
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
    
    /**
     * Basic message listener for default topic
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.default:${KafkaConstants.Topics.DEFAULT}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.DEFAULT}}"
    )
    fun consumeMessage(message: String) {
        logger.debug("Received message from default topic: {}", message.take(100))
        
        try {
            val kafkaMessage = objectMapper.readValue(message, KafkaMessage::class.java)
            messageProcessorService.processMessage(kafkaMessage)
        } catch (e: Exception) {
            logger.error("Error processing message from default topic: {}", e.message)
            messageProcessorService.handleError(message, e, KafkaConstants.Topics.DEFAULT)
        }
    }
    
    /**
     * User events listener
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.user-events:${KafkaConstants.Topics.USER_EVENTS}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.USER_SERVICE}}"
    )
    fun consumeUserEvent(message: String) {
        logger.debug("Received user event: {}", message.take(100))
        
        try {
            val userEvent = objectMapper.readValue(message, UserEvent::class.java)
            messageProcessorService.processUserEvent(userEvent)
        } catch (e: Exception) {
            logger.error("Error processing user event: {}", e.message)
            messageProcessorService.handleError(message, e, KafkaConstants.Topics.USER_EVENTS)
        }
    }
    
    /**
     * Transaction events listener
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.transaction-events:${KafkaConstants.Topics.TRANSACTION_EVENTS}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.TRANSACTION_SERVICE}}"
    )
    fun consumeTransactionEvent(message: String) {
        logger.debug("Received transaction event: {}", message.take(100))
        
        try {
            val transactionEvent = objectMapper.readValue(message, TransactionEvent::class.java)
            messageProcessorService.processTransactionEvent(transactionEvent)
        } catch (e: Exception) {
            logger.error("Error processing transaction event: {}", e.message)
            messageProcessorService.handleError(message, e, KafkaConstants.Topics.TRANSACTION_EVENTS)
        }
    }
    
    /**
     * Listener with full metadata access
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.notifications:${KafkaConstants.Topics.NOTIFICATIONS}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.NOTIFICATION_SERVICE}}"
    )
    fun consumeWithMetadata(
        @Payload message: String,
        @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
        @Header(KafkaHeaders.RECEIVED_PARTITION) partition: Int,
        @Header(KafkaHeaders.OFFSET) offset: Long,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) timestamp: Long
    ) {
        logger.debug("Received message from topic '{}', partition: {}, offset: {}, timestamp: {}", 
            topic, partition, offset, timestamp)
        
        try {
            val kafkaMessage = objectMapper.readValue(message, KafkaMessage::class.java)
            messageProcessorService.processMessageWithMetadata(
                kafkaMessage, topic, partition, offset, timestamp
            )
        } catch (e: Exception) {
            logger.error("Error processing message with metadata from topic '{}': {}", topic, e.message)
            messageProcessorService.handleError(message, e, topic)
        }
    }
    
    /**
     * Manual acknowledgment listener for critical messages
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.audit-logs:${KafkaConstants.Topics.AUDIT_LOGS}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.AUDIT_SERVICE}}",
        containerFactory = "kafkaManualAckListenerContainerFactory"
    )
    fun consumeWithManualAck(message: String, acknowledgment: Acknowledgment) {
        logger.debug("Received audit log message for manual processing")
        
        try {
            val kafkaMessage = objectMapper.readValue(message, KafkaMessage::class.java)
            val processed = messageProcessorService.processMessageWithValidation(kafkaMessage)
            
            if (processed) {
                acknowledgment.acknowledge()
                logger.debug("Audit log message acknowledged: messageId={}", kafkaMessage.id)
            } else {
                logger.warn("Audit log message not acknowledged, will be redelivered: messageId={}", 
                    kafkaMessage.id)
            }
        } catch (e: Exception) {
            logger.error("Error processing audit log message with manual ack: {}", e.message)
            messageProcessorService.handleError(message, e, KafkaConstants.Topics.AUDIT_LOGS)
            // Don't acknowledge on error - message will be redelivered
        }
    }
    
    /**
     * Batch processing listener for high throughput
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.batch-processing:${KafkaConstants.Topics.BATCH_PROCESSING}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.BATCH_PROCESSOR}}",
        containerFactory = "kafkaBatchListenerContainerFactory"
    )
    fun consumeBatch(messages: List<String>) {
        logger.info("Received batch of {} messages for batch processing", messages.size)
        
        val kafkaMessages = messages.mapNotNull { message ->
            try {
                objectMapper.readValue(message, KafkaMessage::class.java)
            } catch (e: Exception) {
                logger.error("Error parsing message in batch: {}", e.message)
                messageProcessorService.handleError(message, e, KafkaConstants.Topics.BATCH_PROCESSING)
                null
            }
        }
        
        if (kafkaMessages.isNotEmpty()) {
            messageProcessorService.processBatch(kafkaMessages)
        }
    }
    
    /**
     * Consumer with full record access for advanced processing
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.real-time-events:${KafkaConstants.Topics.REAL_TIME_EVENTS}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.REAL_TIME_PROCESSOR}}"
    )
    fun consumeRecord(record: ConsumerRecord<String, String>) {
        logger.debug("Received real-time event: key={}, partition={}, offset={}", 
            record.key(), record.partition(), record.offset())
        
        // Extract headers
        val headers = mutableMapOf<String, String>()
        record.headers().forEach { header ->
            headers[header.key()] = String(header.value())
        }
        
        try {
            val kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage::class.java)
            messageProcessorService.processRecord(
                key = record.key(),
                message = kafkaMessage,
                partition = record.partition(),
                offset = record.offset(),
                timestamp = record.timestamp(),
                headers = headers
            )
        } catch (e: Exception) {
            logger.error("Error processing real-time event record: {}", e.message)
            messageProcessorService.handleError(record.value(), e, KafkaConstants.Topics.REAL_TIME_EVENTS)
        }
    }
    
    /**
     * Error events listener - processes messages from DLQ or error topic
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.error-events:${KafkaConstants.Topics.ERROR_EVENTS}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.ERROR_HANDLER}}"
    )
    fun consumeErrorEvent(message: String) {
        logger.info("Received error event for processing: {}", message.take(100))
        
        try {
            // Error events might have different structure, handle accordingly
            val errorData = objectMapper.readTree(message)
            messageProcessorService.processErrorEvent(errorData)
        } catch (e: Exception) {
            logger.error("Error processing error event: {}", e.message)
            // For error events, we might want different handling
        }
    }
    
    /**
     * Metrics listener for monitoring
     */
    @KafkaListener(
        topics = ["\${app.kafka.topics.metrics:${KafkaConstants.Topics.METRICS}}"],
        groupId = "\${kafka.consumer.group-id:${KafkaConstants.ConsumerGroups.MONITORING_SERVICE}}"
    )
    fun consumeMetrics(message: String) {
        logger.debug("Received metrics message")
        
        try {
            val metricsData = objectMapper.readTree(message)
            messageProcessorService.processMetrics(metricsData)
        } catch (e: Exception) {
            logger.error("Error processing metrics: {}", e.message)
        }
    }
}