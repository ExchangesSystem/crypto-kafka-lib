package com.crp.kafka.consumer.service

import com.crp.kafka.shared.dto.*
import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

@Service
class MessageProcessorService {
    
    private val logger = LoggerFactory.getLogger(MessageProcessorService::class.java)
    private val startTime = Instant.now()
    
    // Statistics and tracking
    private val processedMessages = ConcurrentHashMap<String, ProcessedMessage>()
    private val messageCounter = AtomicLong(0)
    private val errorCounter = AtomicLong(0)
    private val totalProcessingTime = AtomicLong(0)
    private val lastProcessedTimestamp = AtomicLong(0)
    
    // Topic-specific counters
    private val topicCounters = ConcurrentHashMap<String, AtomicLong>()
    
    /**
     * Process basic Kafka message
     */
    fun processMessage(message: KafkaMessage) {
        val startTime = System.currentTimeMillis()
        
        logger.info("Processing message: id={}, payload={}", message.id, message.payload.take(100))
        
        try {
            // Simulate processing time
            Thread.sleep(50)
            
            // Store processed message
            val processingTime = System.currentTimeMillis() - startTime
            storeProcessedMessage(message, "default", 0, 0, processingTime, true)
            
            // Update statistics
            updateStatistics(processingTime)
            
            logger.info("Message processed successfully: id={}, processingTime={}ms", 
                message.id, processingTime)
                
        } catch (e: Exception) {
            errorCounter.incrementAndGet()
            val processingTime = System.currentTimeMillis() - startTime
            storeProcessedMessage(message, "default", 0, 0, processingTime, false, e.message)
            
            logger.error("Error processing message: id={}, error={}", message.id, e.message)
            throw e
        }
    }
    
    /**
     * Process user event
     */
    fun processUserEvent(userEvent: UserEvent) {
        val startTime = System.currentTimeMillis()
        
        logger.info("Processing user event: userId={}, eventType={}", 
            userEvent.userId, userEvent.eventType)
        
        try {
            when (userEvent.eventType) {
                EventType.USER_CREATED -> handleUserCreated(userEvent)
                EventType.USER_UPDATED -> handleUserUpdated(userEvent)
                EventType.USER_DELETED -> handleUserDeleted(userEvent)
                EventType.MESSAGE_SENT -> handleMessageSent(userEvent)
                EventType.USER_ACTION -> handleUserAction(userEvent)
                EventType.SESSION_STARTED -> handleSessionStarted(userEvent)
                EventType.SESSION_ENDED -> handleSessionEnded(userEvent)
                EventType.LOGIN_ATTEMPT -> handleLoginAttempt(userEvent)
                EventType.LOGOUT -> handleLogout(userEvent)
            }
            
            // Update statistics
            val processingTime = System.currentTimeMillis() - startTime
            updateStatistics(processingTime)
            incrementTopicCounter("user-events")
            
            logger.info("User event processed successfully: userId={}, eventType={}, processingTime={}ms",
                userEvent.userId, userEvent.eventType, processingTime)
                
        } catch (e: Exception) {
            errorCounter.incrementAndGet()
            logger.error("Error processing user event: userId={}, eventType={}, error={}", 
                userEvent.userId, userEvent.eventType, e.message)
            throw e
        }
    }
    
    /**
     * Process transaction event
     */
    fun processTransactionEvent(transactionEvent: TransactionEvent) {
        val startTime = System.currentTimeMillis()
        
        logger.info("Processing transaction event: transactionId={}, eventType={}, status={}", 
            transactionEvent.transactionId, transactionEvent.eventType, transactionEvent.status)
        
        try {
            when (transactionEvent.eventType) {
                TransactionEventType.TRANSACTION_CREATED -> handleTransactionCreated(transactionEvent)
                TransactionEventType.TRANSACTION_PROCESSED -> handleTransactionProcessed(transactionEvent)
                TransactionEventType.TRANSACTION_COMPLETED -> handleTransactionCompleted(transactionEvent)
                TransactionEventType.TRANSACTION_FAILED -> handleTransactionFailed(transactionEvent)
                TransactionEventType.TRANSACTION_CANCELLED -> handleTransactionCancelled(transactionEvent)
            }
            
            // Update statistics
            val processingTime = System.currentTimeMillis() - startTime
            updateStatistics(processingTime)
            incrementTopicCounter("transaction-events")
            
            logger.info("Transaction event processed successfully: transactionId={}, eventType={}, processingTime={}ms",
                transactionEvent.transactionId, transactionEvent.eventType, processingTime)
                
        } catch (e: Exception) {
            errorCounter.incrementAndGet()
            logger.error("Error processing transaction event: transactionId={}, eventType={}, error={}", 
                transactionEvent.transactionId, transactionEvent.eventType, e.message)
            throw e
        }
    }
    
    /**
     * Process message with metadata
     */
    fun processMessageWithMetadata(
        message: KafkaMessage,
        topic: String,
        partition: Int,
        offset: Long,
        timestamp: Long
    ) {
        val startTime = System.currentTimeMillis()
        
        logger.info("Processing message with metadata: messageId={}, topic={}, partition={}, offset={}", 
            message.id, topic, partition, offset)
        
        try {
            // Calculate lag
            val currentTime = System.currentTimeMillis()
            val lag = currentTime - timestamp
            
            logger.info("Message lag: {}ms", lag)
            
            // Process based on topic
            when (topic) {
                "notifications" -> handleNotification(message)
                else -> processMessage(message)
            }
            
            // Store with metadata
            val processingTime = System.currentTimeMillis() - startTime
            storeProcessedMessage(message, topic, partition, offset, processingTime, true)
            
            // Update statistics
            updateStatistics(processingTime)
            incrementTopicCounter(topic)
            
        } catch (e: Exception) {
            errorCounter.incrementAndGet()
            val processingTime = System.currentTimeMillis() - startTime
            storeProcessedMessage(message, topic, partition, offset, processingTime, false, e.message)
            throw e
        }
    }
    
    /**
     * Process message with validation
     */
    fun processMessageWithValidation(message: KafkaMessage): Boolean {
        logger.info("Validating and processing message: id={}", message.id)
        
        try {
            // Validation checks
            if (message.payload.isBlank()) {
                logger.warn("Invalid message: empty payload, messageId={}", message.id)
                return false
            }
            
            if (processedMessages.containsKey(message.id)) {
                logger.warn("Duplicate message detected: messageId={}", message.id)
                return true // Already processed, but return true to acknowledge
            }
            
            if (message.payload.length > 10000) {
                logger.warn("Message too large: messageId={}, size={}", message.id, message.payload.length)
                return false
            }
            
            // Process if valid
            processMessage(message)
            return true
            
        } catch (e: Exception) {
            logger.error("Validation or processing failed for message: messageId={}, error={}", 
                message.id, e.message)
            return false
        }
    }
    
    /**
     * Process batch of messages
     */
    fun processBatch(messages: List<KafkaMessage>) {
        val startTime = System.currentTimeMillis()
        logger.info("Processing batch of {} messages", messages.size)
        
        var successCount = 0
        var failureCount = 0
        
        messages.forEach { message ->
            try {
                processMessage(message)
                successCount++
            } catch (e: Exception) {
                failureCount++
                logger.error("Error processing message in batch: messageId={}, error={}", 
                    message.id, e.message)
            }
        }
        
        val processingTime = System.currentTimeMillis() - startTime
        val averageTime = if (messages.isNotEmpty()) processingTime / messages.size else 0
        
        logger.info("Batch processed: total={}, success={}, failure={}, totalTime={}ms, avgTime={}ms", 
            messages.size, successCount, failureCount, processingTime, averageTime)
        
        incrementTopicCounter("batch-processing")
    }
    
    /**
     * Process record with full metadata
     */
    fun processRecord(
        key: String?,
        message: KafkaMessage,
        partition: Int,
        offset: Long,
        timestamp: Long,
        headers: Map<String, String>
    ) {
        logger.info("Processing record with key: key={}, messageId={}, headers={}", 
            key, message.id, headers.size)
        
        try {
            // Use key for routing or partitioning logic
            when {
                key?.startsWith("priority-") == true -> processPriorityMessage(message)
                key?.startsWith("user-") == true -> processUserSpecificMessage(key, message)
                headers.containsKey("urgent") -> processUrgentMessage(message)
                else -> processMessage(message)
            }
            
            incrementTopicCounter("real-time-events")
            
        } catch (e: Exception) {
            logger.error("Error processing record: key={}, messageId={}, error={}", 
                key, message.id, e.message)
            throw e
        }
    }
    
    /**
     * Handle error events
     */
    fun handleError(rawMessage: String, error: Exception, topic: String) {
        errorCounter.incrementAndGet()
        
        logger.error("Error handling message from topic '{}'. Total errors: {}. Error: {}", 
            topic, errorCounter.get(), error.message)
        
        // In production, you would:
        // 1. Send to Dead Letter Queue (DLQ)
        // 2. Store in database for manual review
        // 3. Send alerts if error rate exceeds threshold
        // 4. Implement retry logic with exponential backoff
        
        // For now, just log and increment counter
        incrementTopicCounter("$topic-errors")
    }
    
    /**
     * Process error events from error topic
     */
    fun processErrorEvent(errorData: JsonNode) {
        logger.info("Processing error event: {}", errorData.toString().take(100))
        
        try {
            // Extract error information
            val errorType = errorData.get("errorType")?.asText() ?: "UNKNOWN"
            val originalTopic = errorData.get("originalTopic")?.asText() ?: "unknown"
            val retryCount = errorData.get("retryCount")?.asInt() ?: 0
            
            logger.info("Error event details: type={}, originalTopic={}, retryCount={}", 
                errorType, originalTopic, retryCount)
            
            // Handle based on error type
            when (errorType) {
                "SERIALIZATION_ERROR" -> handleSerializationError(errorData)
                "VALIDATION_ERROR" -> handleValidationError(errorData)
                "PROCESSING_ERROR" -> handleProcessingError(errorData)
                else -> handleGenericError(errorData)
            }
            
            incrementTopicCounter("error-events")
            
        } catch (e: Exception) {
            logger.error("Error processing error event: {}", e.message)
        }
    }
    
    /**
     * Process metrics
     */
    fun processMetrics(metricsData: JsonNode) {
        logger.debug("Processing metrics: {}", metricsData.toString().take(50))
        
        try {
            // Extract metrics
            val metricType = metricsData.get("type")?.asText()
            val value = metricsData.get("value")?.asDouble()
            val timestamp = metricsData.get("timestamp")?.asLong()
            
            logger.debug("Metrics: type={}, value={}, timestamp={}", metricType, value, timestamp)
            
            // Store or forward metrics to monitoring system
            // Implementation depends on your monitoring stack
            
            incrementTopicCounter("metrics")
            
        } catch (e: Exception) {
            logger.error("Error processing metrics: {}", e.message)
        }
    }
    
    // Event-specific handlers
    private fun handleUserCreated(event: UserEvent) {
        logger.info("User created: userId={}", event.userId)
        // Implement user creation side effects
    }
    
    private fun handleUserUpdated(event: UserEvent) {
        logger.info("User updated: userId={}", event.userId)
        // Implement user update side effects
    }
    
    private fun handleUserDeleted(event: UserEvent) {
        logger.info("User deleted: userId={}", event.userId)
        // Implement user deletion cleanup
    }
    
    private fun handleMessageSent(event: UserEvent) {
        logger.info("Message sent by user: userId={}", event.userId)
        val messageId = event.data["messageId"] as? String
        val message = event.data["message"] as? String
        logger.info("Message details: messageId={}, content={}", messageId, message?.take(50))
    }
    
    private fun handleUserAction(event: UserEvent) {
        logger.info("User action: userId={}, data={}", event.userId, event.data)
        // Implement generic user action handling
    }
    
    private fun handleSessionStarted(event: UserEvent) {
        logger.info("Session started: userId={}, sessionId={}", event.userId, event.sessionId)
    }
    
    private fun handleSessionEnded(event: UserEvent) {
        logger.info("Session ended: userId={}, sessionId={}", event.userId, event.sessionId)
    }
    
    private fun handleLoginAttempt(event: UserEvent) {
        logger.info("Login attempt: userId={}", event.userId)
        // Implement login attempt tracking/security
    }
    
    private fun handleLogout(event: UserEvent) {
        logger.info("User logout: userId={}", event.userId)
    }
    
    // Transaction event handlers
    private fun handleTransactionCreated(event: TransactionEvent) {
        logger.info("Transaction created: transactionId={}, amount={} {}", 
            event.transactionId, event.amount, event.currency)
    }
    
    private fun handleTransactionProcessed(event: TransactionEvent) {
        logger.info("Transaction processed: transactionId={}", event.transactionId)
    }
    
    private fun handleTransactionCompleted(event: TransactionEvent) {
        logger.info("Transaction completed: transactionId={}", event.transactionId)
    }
    
    private fun handleTransactionFailed(event: TransactionEvent) {
        logger.warn("Transaction failed: transactionId={}", event.transactionId)
    }
    
    private fun handleTransactionCancelled(event: TransactionEvent) {
        logger.info("Transaction cancelled: transactionId={}", event.transactionId)
    }
    
    // Specialized message handlers
    private fun handleNotification(message: KafkaMessage) {
        logger.info("Processing notification: messageId={}", message.id)
        // Implement notification-specific logic
        processMessage(message)
    }
    
    private fun processPriorityMessage(message: KafkaMessage) {
        logger.info("Processing priority message: messageId={}", message.id)
        // Implement priority processing (faster, different queue, etc.)
        processMessage(message)
    }
    
    private fun processUserSpecificMessage(key: String, message: KafkaMessage) {
        val userId = key.substring("user-".length)
        logger.info("Processing user-specific message: userId={}, messageId={}", userId, message.id)
        // Implement user-specific processing
        processMessage(message)
    }
    
    private fun processUrgentMessage(message: KafkaMessage) {
        logger.warn("Processing urgent message: messageId={}", message.id)
        // Implement urgent processing (alerts, notifications, etc.)
        processMessage(message)
    }
    
    // Error handlers
    private fun handleSerializationError(errorData: JsonNode) {
        logger.error("Handling serialization error: {}", errorData.toString().take(100))
    }
    
    private fun handleValidationError(errorData: JsonNode) {
        logger.error("Handling validation error: {}", errorData.toString().take(100))
    }
    
    private fun handleProcessingError(errorData: JsonNode) {
        logger.error("Handling processing error: {}", errorData.toString().take(100))
    }
    
    private fun handleGenericError(errorData: JsonNode) {
        logger.error("Handling generic error: {}", errorData.toString().take(100))
    }
    
    // Utility methods
    private fun storeProcessedMessage(
        message: KafkaMessage,
        topic: String,
        partition: Int,
        offset: Long,
        processingTime: Long,
        successful: Boolean,
        errorMessage: String? = null
    ) {
        processedMessages[message.id] = ProcessedMessage(
            messageId = message.id,
            originalPayload = message.payload,
            processedAt = Instant.now(),
            processingTimeMs = processingTime,
            topic = topic,
            partition = partition,
            offset = offset,
            successful = successful,
            errorMessage = errorMessage
        )
        
        // Keep only last 1000 messages to prevent memory issues
        if (processedMessages.size > 1000) {
            val oldestKey = processedMessages.keys.first()
            processedMessages.remove(oldestKey)
        }
    }
    
    private fun updateStatistics(processingTime: Long) {
        messageCounter.incrementAndGet()
        totalProcessingTime.addAndGet(processingTime)
        lastProcessedTimestamp.set(System.currentTimeMillis())
    }
    
    private fun incrementTopicCounter(topic: String) {
        topicCounters.computeIfAbsent(topic) { AtomicLong(0) }.incrementAndGet()
    }
    
    // Public methods for monitoring
    fun getStatistics(): ConsumerStatistics {
        val totalProcessed = messageCounter.get()
        val totalErrors = errorCounter.get()
        val errorRate = if (totalProcessed > 0) 
            totalErrors.toDouble() / (totalProcessed + totalErrors) * 100 else 0.0
        val averageProcessingTime = if (totalProcessed > 0) 
            totalProcessingTime.get().toDouble() / totalProcessed else 0.0
        val uptime = Duration.between(startTime, Instant.now())
        val uptimeString = "${uptime.toDays()}d ${uptime.toHoursPart()}h ${uptime.toMinutesPart()}m"
        
        return ConsumerStatistics(
            totalProcessed = totalProcessed,
            totalErrors = totalErrors,
            errorRate = errorRate,
            averageProcessingTimeMs = averageProcessingTime,
            lastProcessedTimestamp = if (lastProcessedTimestamp.get() > 0) 
                Instant.ofEpochMilli(lastProcessedTimestamp.get()) else null,
            uptime = uptimeString,
            topicsConsumed = topicCounters.keys.toList()
        )
    }
    
    fun getProcessedMessage(messageId: String): ProcessedMessage? {
        return processedMessages[messageId]
    }
    
    fun clearCache() {
        processedMessages.clear()
        logger.info("Message cache cleared")
    }
    
    fun getTopicStatistics(): Map<String, Long> {
        return topicCounters.mapValues { it.value.get() }
    }
}