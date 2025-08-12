package com.crp.kafka.producer.service

import com.crp.kafka.shared.constants.KafkaConstants
import com.crp.kafka.shared.dto.*
import com.crp.system.libs.kafka.KafkaMessageProducer
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicLong

@Service
class MessageProducerService(
    @Autowired
    private val kafkaMessageProducer: KafkaMessageProducer,
    @Autowired
    private val objectMapper: ObjectMapper
) {
    
    private val logger = LoggerFactory.getLogger(MessageProducerService::class.java)
    private val startTime = Instant.now()
    
    // Statistics
    private val sentMessages = AtomicLong(0)
    private val failedMessages = AtomicLong(0)
    private val totalProcessingTime = AtomicLong(0)
    
    @Value("\${app.kafka.topics.default:${KafkaConstants.Topics.DEFAULT}}")
    private lateinit var defaultTopic: String
    
    @Value("\${app.kafka.topics.user-events:${KafkaConstants.Topics.USER_EVENTS}}")
    private lateinit var userEventsTopic: String
    
    @Value("\${app.kafka.topics.transaction-events:${KafkaConstants.Topics.TRANSACTION_EVENTS}}")
    private lateinit var transactionEventsTopic: String
    
    /**
     * Send a simple fire-and-forget message
     */
    fun sendSimpleMessage(message: String, correlationId: String? = null): String {
        val startTime = System.currentTimeMillis()
        val messageId = UUID.randomUUID().toString()
        
        return try {
            val kafkaMessage = KafkaMessage(
                id = messageId,
                payload = message,
                correlationId = correlationId,
                source = "kafka-producer-app"
            )
            
            val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
            val record = createProducerRecord(defaultTopic, messageId, jsonMessage, mapOf(
                KafkaConstants.Headers.MESSAGE_ID to messageId,
                KafkaConstants.Headers.CORRELATION_ID to (correlationId ?: ""),
                KafkaConstants.Headers.SOURCE_SERVICE to "kafka-producer-app",
                KafkaConstants.Headers.MESSAGE_TYPE to "SimpleMessage"
            ))
            
            logger.info("Sending simple message to topic '{}': messageId={}", defaultTopic, messageId)
            kafkaMessageProducer.sendMessage(defaultTopic, jsonMessage)
            
            // Update statistics
            sentMessages.incrementAndGet()
            totalProcessingTime.addAndGet(System.currentTimeMillis() - startTime)
            
            messageId
        } catch (e: Exception) {
            failedMessages.incrementAndGet()
            logger.error("Failed to send simple message: messageId={}, error={}", messageId, e.message)
            throw e
        }
    }
    
    /**
     * Send message with acknowledgment
     */
    fun sendMessageWithResult(message: String): Mono<SendResult<String?, String?>> {
        val startTime = System.currentTimeMillis()
        val messageId = UUID.randomUUID().toString()
        
        val kafkaMessage = KafkaMessage(
            id = messageId,
            payload = message,
            source = "kafka-producer-app"
        )
        
        val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
        
        logger.info("Sending message with result to topic '{}': messageId={}", defaultTopic, messageId)
        
        val future: CompletableFuture<SendResult<String?, String?>?> = 
            kafkaMessageProducer.sendMessageWithResult(defaultTopic, jsonMessage)
        
        return Mono.fromFuture(future)
            .doOnSuccess { result ->
                sentMessages.incrementAndGet()
                totalProcessingTime.addAndGet(System.currentTimeMillis() - startTime)
                
                logger.info("Message sent successfully: messageId={}, partition={}, offset={}", 
                    messageId,
                    result?.recordMetadata?.partition(), 
                    result?.recordMetadata?.offset()
                )
            }
            .doOnError { error ->
                failedMessages.incrementAndGet()
                logger.error("Failed to send message with result: messageId={}, error={}", 
                    messageId, error.message)
            }
            .map { it!! }
    }
    
    /**
     * Send structured user event
     */
    fun sendUserEvent(userId: String, message: String, metadata: Map<String, Any>): String {
        val startTime = System.currentTimeMillis()
        val messageId = UUID.randomUUID().toString()
        
        return try {
            val userEvent = UserEvent(
                userId = userId,
                eventType = EventType.MESSAGE_SENT,
                data = mapOf(
                    "message" to message,
                    "metadata" to metadata,
                    "messageId" to messageId
                )
            )
            
            val jsonMessage = objectMapper.writeValueAsString(userEvent)
            
            logger.info("Sending user event to topic '{}': messageId={}, userId={}", 
                userEventsTopic, messageId, userId)
            
            kafkaMessageProducer.sendMessage(userEventsTopic, jsonMessage)
            
            // Update statistics
            sentMessages.incrementAndGet()
            totalProcessingTime.addAndGet(System.currentTimeMillis() - startTime)
            
            messageId
        } catch (e: Exception) {
            failedMessages.incrementAndGet()
            logger.error("Failed to send user event: messageId={}, userId={}, error={}", 
                messageId, userId, e.message)
            throw e
        }
    }
    
    /**
     * Send transaction event
     */
    fun sendTransactionEvent(transactionEvent: TransactionEvent): String {
        val startTime = System.currentTimeMillis()
        val messageId = UUID.randomUUID().toString()
        
        return try {
            val jsonMessage = objectMapper.writeValueAsString(transactionEvent)
            
            logger.info("Sending transaction event to topic '{}': messageId={}, transactionId={}, eventType={}", 
                transactionEventsTopic, messageId, transactionEvent.transactionId, transactionEvent.eventType)
            
            kafkaMessageProducer.sendMessage(transactionEventsTopic, jsonMessage)
            
            // Update statistics
            sentMessages.incrementAndGet()
            totalProcessingTime.addAndGet(System.currentTimeMillis() - startTime)
            
            messageId
        } catch (e: Exception) {
            failedMessages.incrementAndGet()
            logger.error("Failed to send transaction event: messageId={}, transactionId={}, error={}", 
                messageId, transactionEvent.transactionId, e.message)
            throw e
        }
    }
    
    /**
     * Send message to specific topic
     */
    fun sendToTopic(topic: String, message: String, key: String? = null, headers: Map<String, String> = emptyMap()): String {
        val startTime = System.currentTimeMillis()
        val messageId = UUID.randomUUID().toString()
        
        return try {
            val kafkaMessage = KafkaMessage(
                id = messageId,
                payload = message,
                headers = headers,
                source = "kafka-producer-app"
            )
            
            val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
            
            logger.info("Sending message to custom topic '{}': messageId={}, hasKey={}", 
                topic, messageId, key != null)
            
            // If we need to send with key, we'd need to enhance the KafkaMessageProducer
            // For now, using the simple send method
            kafkaMessageProducer.sendMessage(topic, jsonMessage)
            
            // Update statistics
            sentMessages.incrementAndGet()
            totalProcessingTime.addAndGet(System.currentTimeMillis() - startTime)
            
            messageId
        } catch (e: Exception) {
            failedMessages.incrementAndGet()
            logger.error("Failed to send message to topic '{}': messageId={}, error={}", 
                topic, messageId, e.message)
            throw e
        }
    }
    
    /**
     * Send message with correlation ID for request-reply patterns
     */
    fun sendWithCorrelationId(message: String, correlationId: String, replyTopic: String? = null): String {
        val startTime = System.currentTimeMillis()
        val messageId = UUID.randomUUID().toString()
        
        return try {
            val kafkaMessage = KafkaMessage(
                id = messageId,
                payload = message,
                correlationId = correlationId,
                headers = replyTopic?.let { mapOf("reply-topic" to it) },
                source = "kafka-producer-app"
            )
            
            val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
            
            logger.info("Sending message with correlation ID '{}': messageId={}", correlationId, messageId)
            
            kafkaMessageProducer.sendMessage(defaultTopic, jsonMessage)
            
            // Update statistics
            sentMessages.incrementAndGet()
            totalProcessingTime.addAndGet(System.currentTimeMillis() - startTime)
            
            messageId
        } catch (e: Exception) {
            failedMessages.incrementAndGet()
            logger.error("Failed to send message with correlation ID '{}': messageId={}, error={}", 
                correlationId, messageId, e.message)
            throw e
        }
    }
    
    /**
     * Health check
     */
    fun isHealthy(): Boolean {
        return try {
            // Simple health check - you could enhance this with actual Kafka connectivity check
            val testMessage = "health-check-${System.currentTimeMillis()}"
            sendSimpleMessage(testMessage)
            true
        } catch (e: Exception) {
            logger.error("Health check failed: {}", e.message)
            false
        }
    }
    
    /**
     * Get uptime
     */
    fun getUptime(): String {
        val duration = Duration.between(startTime, Instant.now())
        return "${duration.toDays()}d ${duration.toHoursPart()}h ${duration.toMinutesPart()}m ${duration.toSecondsPart()}s"
    }
    
    /**
     * Get producer statistics
     */
    fun getStatistics(): Map<String, Any> {
        val totalMessages = sentMessages.get() + failedMessages.get()
        val averageProcessingTime = if (sentMessages.get() > 0) 
            totalProcessingTime.get() / sentMessages.get() else 0
        
        return mapOf(
            "totalSent" to sentMessages.get(),
            "totalFailed" to failedMessages.get(),
            "successRate" to if (totalMessages > 0) 
                (sentMessages.get().toDouble() / totalMessages * 100) else 100.0,
            "averageProcessingTimeMs" to averageProcessingTime,
            "uptime" to getUptime(),
            "startTime" to startTime.toString(),
            "configuredTopics" to mapOf(
                "default" to defaultTopic,
                "userEvents" to userEventsTopic,
                "transactionEvents" to transactionEventsTopic
            )
        )
    }
    
    /**
     * Helper method to create ProducerRecord with headers
     */
    private fun createProducerRecord(topic: String, key: String?, value: String, headers: Map<String, String>): ProducerRecord<String, String> {
        val record = ProducerRecord<String, String>(topic, key, value)
        
        headers.forEach { (headerKey, headerValue) ->
            if (headerValue.isNotEmpty()) {
                record.headers().add(RecordHeader(headerKey, headerValue.toByteArray()))
            }
        }
        
        return record
    }
}