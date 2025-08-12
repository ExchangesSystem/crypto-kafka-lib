package com.crp.kafka.producer.controller

import com.crp.kafka.producer.service.MessageProducerService
import com.crp.kafka.shared.constants.KafkaConstants
import com.crp.kafka.shared.dto.*
import jakarta.validation.Valid
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.validation.annotation.Validated
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/v1/messages")
@Validated
class MessageController(
    @Autowired
    private val messageProducerService: MessageProducerService
) {
    
    private val logger = LoggerFactory.getLogger(MessageController::class.java)
    
    /**
     * Send a simple message to the default topic
     */
    @PostMapping("/send")
    fun sendMessage(@Valid @RequestBody request: SimpleMessageRequest): ResponseEntity<MessageResponse> {
        return try {
            logger.info("Received request to send message: {}", request.message.take(50))
            val messageId = messageProducerService.sendSimpleMessage(request.message)
            
            ResponseEntity.ok(MessageResponse(
                success = true,
                messageId = messageId,
                metadata = mapOf(
                    "topic" to KafkaConstants.Topics.DEFAULT,
                    "messageLength" to request.message.length
                )
            ))
        } catch (e: Exception) {
            logger.error("Error sending message: {}", e.message)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(MessageResponse(
                    success = false,
                    error = e.message
                ))
        }
    }
    
    /**
     * Send message and wait for broker acknowledgment
     */
    @PostMapping("/send-with-ack")
    fun sendMessageWithAck(@Valid @RequestBody request: SimpleMessageRequest): Mono<ResponseEntity<AsyncMessageResponse>> {
        logger.info("Received request to send message with acknowledgment")
        
        return messageProducerService.sendMessageWithResult(request.message)
            .map { result ->
                ResponseEntity.ok(AsyncMessageResponse(
                    acknowledged = true,
                    partition = result.recordMetadata?.partition(),
                    offset = result.recordMetadata?.offset(),
                    timestamp = result.recordMetadata?.timestamp(),
                    messageId = result.producerRecord.headers()
                        .find { it.key() == KafkaConstants.Headers.MESSAGE_ID }
                        ?.value()?.let { String(it) }
                ))
            }
            .onErrorResume { error ->
                logger.error("Failed to send message with acknowledgment: {}", error.message)
                Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(AsyncMessageResponse(
                        acknowledged = false,
                        error = error.message
                    )))
            }
    }
    
    /**
     * Send structured user event
     */
    @PostMapping("/user-events")
    fun sendUserEvent(@Valid @RequestBody request: UserMessageRequest): ResponseEntity<MessageResponse> {
        return try {
            logger.info("Received user event: userId={}, messageLength={}", 
                request.userId, request.message.length)
            
            val messageId = messageProducerService.sendUserEvent(
                userId = request.userId,
                message = request.message,
                metadata = request.metadata ?: emptyMap()
            )
            
            ResponseEntity.ok(MessageResponse(
                success = true,
                messageId = messageId,
                metadata = mapOf(
                    "topic" to KafkaConstants.Topics.USER_EVENTS,
                    "userId" to request.userId
                )
            ))
        } catch (e: Exception) {
            logger.error("Error sending user event: {}", e.message)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(MessageResponse(
                    success = false,
                    error = e.message
                ))
        }
    }
    
    /**
     * Send transaction event
     */
    @PostMapping("/transaction-events")
    fun sendTransactionEvent(@Valid @RequestBody request: TransactionEvent): ResponseEntity<MessageResponse> {
        return try {
            logger.info("Received transaction event: transactionId={}, eventType={}", 
                request.transactionId, request.eventType)
            
            val messageId = messageProducerService.sendTransactionEvent(request)
            
            ResponseEntity.ok(MessageResponse(
                success = true,
                messageId = messageId,
                metadata = mapOf(
                    "topic" to KafkaConstants.Topics.TRANSACTION_EVENTS,
                    "transactionId" to request.transactionId,
                    "eventType" to request.eventType.name
                )
            ))
        } catch (e: Exception) {
            logger.error("Error sending transaction event: {}", e.message)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(MessageResponse(
                    success = false,
                    error = e.message
                ))
        }
    }
    
    /**
     * Send message to specific topic
     */
    @PostMapping("/send-to-topic")
    fun sendToTopic(@Valid @RequestBody request: TopicMessageRequest): ResponseEntity<MessageResponse> {
        return try {
            logger.info("Sending message to custom topic: {}", request.topic)
            
            val messageId = messageProducerService.sendToTopic(
                topic = request.topic,
                message = request.message,
                key = request.key,
                headers = request.headers ?: emptyMap()
            )
            
            ResponseEntity.ok(MessageResponse(
                success = true,
                messageId = messageId,
                metadata = mapOf(
                    "topic" to request.topic,
                    "hasKey" to (request.key != null),
                    "headerCount" to (request.headers?.size ?: 0)
                )
            ))
        } catch (e: Exception) {
            logger.error("Error sending message to topic {}: {}", request.topic, e.message)
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(MessageResponse(
                    success = false,
                    error = e.message
                ))
        }
    }
    
    /**
     * Send multiple messages in batch
     */
    @PostMapping("/batch-send")
    fun batchSend(@Valid @RequestBody messages: List<SimpleMessageRequest>): ResponseEntity<BatchResponse> {
        if (messages.isEmpty()) {
            return ResponseEntity.badRequest()
                .body(BatchResponse(
                    totalSent = 0,
                    successCount = 0,
                    failureCount = 1,
                    results = listOf(MessageResponse(
                        success = false,
                        error = "Empty message list provided"
                    ))
                ))
        }
        
        if (messages.size > 100) {
            return ResponseEntity.badRequest()
                .body(BatchResponse(
                    totalSent = 0,
                    successCount = 0,
                    failureCount = 1,
                    results = listOf(MessageResponse(
                        success = false,
                        error = "Batch size cannot exceed 100 messages"
                    ))
                ))
        }
        
        logger.info("Received batch send request for {} messages", messages.size)
        
        val results = messages.mapIndexed { index, request ->
            try {
                val messageId = messageProducerService.sendSimpleMessage(
                    message = request.message,
                    correlationId = "batch-${System.currentTimeMillis()}-$index"
                )
                MessageResponse(
                    success = true,
                    messageId = messageId
                )
            } catch (e: Exception) {
                logger.error("Error sending message at index {}: {}", index, e.message)
                MessageResponse(
                    success = false,
                    error = e.message
                )
            }
        }
        
        val successCount = results.count { it.success }
        val failureCount = results.count { !it.success }
        
        return ResponseEntity.ok(BatchResponse(
            totalSent = messages.size,
            successCount = successCount,
            failureCount = failureCount,
            results = results
        ))
    }
    
    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    fun health(): ResponseEntity<HealthResponse> {
        return try {
            val isHealthy = messageProducerService.isHealthy()
            
            ResponseEntity.ok(HealthResponse(
                status = if (isHealthy) HealthStatus.UP else HealthStatus.DOWN,
                service = "kafka-producer",
                version = "1.0.0",
                details = mapOf(
                    "kafkaConnected" to isHealthy,
                    "uptime" to messageProducerService.getUptime()
                )
            ))
        } catch (e: Exception) {
            ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(HealthResponse(
                    status = HealthStatus.DOWN,
                    service = "kafka-producer",
                    version = "1.0.0",
                    details = mapOf("error" to (e.message ?: "Unknown error"))
                ))
        }
    }
    
    /**
     * Get producer statistics
     */
    @GetMapping("/stats")
    fun getStatistics(): ResponseEntity<Map<String, Any>> {
        return ResponseEntity.ok(messageProducerService.getStatistics())
    }
}