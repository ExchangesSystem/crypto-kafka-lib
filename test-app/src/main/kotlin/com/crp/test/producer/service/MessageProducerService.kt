package com.crp.test.producer.service

import com.crp.system.libs.kafka.KafkaMessageProducer
import com.crp.test.dto.*
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.support.SendResult
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.util.UUID
import java.util.concurrent.CompletableFuture

@Service
class MessageProducerService(
    @Autowired
    private val kafkaMessageProducer: KafkaMessageProducer,
    @Autowired
    private val objectMapper: ObjectMapper
) {
    
    private val logger = LoggerFactory.getLogger(MessageProducerService::class.java)
    
    @Value("\${kafka.topics.default:test-topic}")
    private lateinit var defaultTopic: String
    
    @Value("\${kafka.topics.user-events:user-events}")
    private lateinit var userEventsTopic: String
    
    // Simple fire-and-forget message
    fun sendSimpleMessage(message: String): String {
        val messageId = UUID.randomUUID().toString()
        val kafkaMessage = KafkaMessage(
            id = messageId,
            payload = message
        )
        
        val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
        logger.info("Sending message to topic '{}': {}", defaultTopic, jsonMessage)
        
        kafkaMessageProducer.sendMessage(defaultTopic, jsonMessage)
        return messageId
    }
    
    // Send message with acknowledgment
    fun sendMessageWithResult(message: String): Mono<SendResult<String?, String?>> {
        val messageId = UUID.randomUUID().toString()
        val kafkaMessage = KafkaMessage(
            id = messageId,
            payload = message
        )
        
        val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
        logger.info("Sending message with result to topic '{}': {}", defaultTopic, jsonMessage)
        
        val future: CompletableFuture<SendResult<String?, String?>?> = 
            kafkaMessageProducer.sendMessageWithResult(defaultTopic, jsonMessage)
        
        return Mono.fromFuture(future)
            .doOnSuccess { result ->
                logger.info("Message sent successfully: partition={}, offset={}", 
                    result?.recordMetadata?.partition(), 
                    result?.recordMetadata?.offset())
            }
            .doOnError { error ->
                logger.error("Failed to send message: {}", error.message)
            }
            .map { it!! }
    }
    
    // Send user event
    fun sendUserEvent(userId: String, message: String, metadata: Map<String, Any>): String {
        val messageId = UUID.randomUUID().toString()
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
        logger.info("Sending user event to topic '{}': {}", userEventsTopic, jsonMessage)
        
        kafkaMessageProducer.sendMessage(userEventsTopic, jsonMessage)
        return messageId
    }
    
    // Send to specific topic
    fun sendToTopic(topic: String, message: String): String {
        val messageId = UUID.randomUUID().toString()
        val kafkaMessage = KafkaMessage(
            id = messageId,
            payload = message
        )
        
        val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
        logger.info("Sending message to custom topic '{}': {}", topic, jsonMessage)
        
        kafkaMessageProducer.sendMessage(topic, jsonMessage)
        return messageId
    }
    
    // Send message with correlation ID (for request-reply pattern)
    fun sendWithCorrelationId(message: String, correlationId: String): String {
        val messageId = UUID.randomUUID().toString()
        val kafkaMessage = KafkaMessage(
            id = messageId,
            payload = message,
            correlationId = correlationId
        )
        
        val jsonMessage = objectMapper.writeValueAsString(kafkaMessage)
        logger.info("Sending message with correlation ID '{}': {}", correlationId, jsonMessage)
        
        kafkaMessageProducer.sendMessage(defaultTopic, jsonMessage)
        return messageId
    }
}