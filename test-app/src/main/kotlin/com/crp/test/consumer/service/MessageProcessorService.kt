package com.crp.test.consumer.service

import com.crp.test.dto.KafkaMessage
import com.crp.test.dto.UserEvent
import com.crp.test.dto.EventType
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

@Service
class MessageProcessorService {
    
    private val logger = LoggerFactory.getLogger(MessageProcessorService::class.java)
    
    // Store processed messages for demonstration
    private val processedMessages = ConcurrentHashMap<String, ProcessedMessage>()
    private val messageCounter = AtomicLong(0)
    private val errorCounter = AtomicLong(0)
    
    data class ProcessedMessage(
        val messageId: String,
        val content: String,
        val processedAt: Instant,
        val processingTimeMs: Long
    )
    
    fun processMessage(message: KafkaMessage) {
        val startTime = System.currentTimeMillis()
        
        logger.info("Processing message: id={}, payload={}", message.id, message.payload)
        
        // Simulate some processing
        Thread.sleep(100)
        
        // Store processed message
        val processingTime = System.currentTimeMillis() - startTime
        processedMessages[message.id] = ProcessedMessage(
            messageId = message.id,
            content = message.payload,
            processedAt = Instant.now(),
            processingTimeMs = processingTime
        )
        
        messageCounter.incrementAndGet()
        logger.info("Message processed successfully in {}ms. Total processed: {}", 
            processingTime, messageCounter.get())
    }
    
    fun processMessageWithMetadata(
        message: KafkaMessage,
        topic: String,
        partition: Int,
        offset: Long,
        timestamp: Long
    ) {
        logger.info("Processing message with metadata from topic: {}, partition: {}, offset: {}", 
            topic, partition, offset)
        
        // Process message with additional context
        processMessage(message)
        
        // You could store metadata for monitoring/analytics
        logger.info("Message timestamp: {}, Current time: {}, Lag: {}ms", 
            timestamp, 
            System.currentTimeMillis(), 
            System.currentTimeMillis() - timestamp)
    }
    
    fun processUserEvent(userEvent: UserEvent) {
        logger.info("Processing user event: userId={}, eventType={}", 
            userEvent.userId, userEvent.eventType)
        
        when (userEvent.eventType) {
            EventType.USER_CREATED -> handleUserCreated(userEvent)
            EventType.USER_UPDATED -> handleUserUpdated(userEvent)
            EventType.USER_DELETED -> handleUserDeleted(userEvent)
            EventType.MESSAGE_SENT -> handleMessageSent(userEvent)
            EventType.USER_ACTION -> handleUserAction(userEvent)
        }
        
        messageCounter.incrementAndGet()
    }
    
    fun processMessageWithValidation(message: KafkaMessage): Boolean {
        logger.info("Validating message: {}", message.id)
        
        // Perform validation
        if (message.payload.isBlank()) {
            logger.warn("Invalid message: empty payload")
            return false
        }
        
        if (processedMessages.containsKey(message.id)) {
            logger.warn("Duplicate message detected: {}", message.id)
            return true // Already processed, acknowledge it
        }
        
        // Process if valid
        processMessage(message)
        return true
    }
    
    fun processBatch(messages: List<KafkaMessage>) {
        val startTime = System.currentTimeMillis()
        logger.info("Processing batch of {} messages", messages.size)
        
        messages.forEach { message ->
            try {
                processMessage(message)
            } catch (e: Exception) {
                logger.error("Error processing message in batch: {}", e.message)
                errorCounter.incrementAndGet()
            }
        }
        
        val processingTime = System.currentTimeMillis() - startTime
        logger.info("Batch processed in {}ms. Average: {}ms per message", 
            processingTime, 
            if (messages.isNotEmpty()) processingTime / messages.size else 0)
    }
    
    fun processRecord(key: String?, message: KafkaMessage) {
        logger.info("Processing record with key: {}", key)
        
        // Use key for partitioning logic if needed
        if (key != null) {
            // Example: Route to different processors based on key
            when {
                key.startsWith("priority-") -> processPriorityMessage(message)
                key.startsWith("batch-") -> queueForBatchProcessing(message)
                else -> processMessage(message)
            }
        } else {
            processMessage(message)
        }
    }
    
    fun handleError(rawMessage: String, error: Exception) {
        errorCounter.incrementAndGet()
        logger.error("Error handling message. Total errors: {}. Message: {}", 
            errorCounter.get(), rawMessage)
        
        // In production, you might want to:
        // 1. Send to a Dead Letter Queue (DLQ)
        // 2. Store in a database for manual review
        // 3. Send alerts if error rate is high
        // 4. Implement retry logic with exponential backoff
    }
    
    // Event-specific handlers
    private fun handleUserCreated(event: UserEvent) {
        logger.info("User created: {}", event.userId)
        // Implement user creation logic
    }
    
    private fun handleUserUpdated(event: UserEvent) {
        logger.info("User updated: {}", event.userId)
        // Implement user update logic
    }
    
    private fun handleUserDeleted(event: UserEvent) {
        logger.info("User deleted: {}", event.userId)
        // Implement user deletion logic
    }
    
    private fun handleMessageSent(event: UserEvent) {
        logger.info("Message sent by user: {}", event.userId)
        val messageId = event.data["messageId"] as? String
        val message = event.data["message"] as? String
        logger.info("Message details: id={}, content={}", messageId, message)
    }
    
    private fun handleUserAction(event: UserEvent) {
        logger.info("User action: userId={}, data={}", event.userId, event.data)
        // Implement generic user action logic
    }
    
    private fun processPriorityMessage(message: KafkaMessage) {
        logger.info("Processing priority message: {}", message.id)
        // Implement priority processing logic
        processMessage(message)
    }
    
    private fun queueForBatchProcessing(message: KafkaMessage) {
        logger.info("Queuing message for batch processing: {}", message.id)
        // Implement batch queuing logic
        processMessage(message)
    }
    
    // Metrics and monitoring
    fun getStatistics(): Map<String, Any> {
        return mapOf(
            "totalProcessed" to messageCounter.get(),
            "totalErrors" to errorCounter.get(),
            "errorRate" to if (messageCounter.get() > 0) 
                errorCounter.get().toDouble() / messageCounter.get() else 0.0,
            "cachedMessages" to processedMessages.size
        )
    }
    
    fun getProcessedMessage(messageId: String): ProcessedMessage? {
        return processedMessages[messageId]
    }
    
    fun clearCache() {
        processedMessages.clear()
        logger.info("Message cache cleared")
    }
}