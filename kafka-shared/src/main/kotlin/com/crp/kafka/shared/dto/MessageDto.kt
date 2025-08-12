package com.crp.kafka.shared.dto

import com.fasterxml.jackson.annotation.JsonProperty
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.NotNull
import java.time.Instant

// Request DTOs
data class SimpleMessageRequest(
    @field:NotBlank(message = "Message cannot be blank")
    @JsonProperty("message")
    val message: String
)

data class UserMessageRequest(
    @field:NotBlank(message = "User ID cannot be blank")
    @JsonProperty("userId")
    val userId: String,
    
    @field:NotBlank(message = "Message cannot be blank")
    @JsonProperty("message")
    val message: String,
    
    @JsonProperty("metadata")
    val metadata: Map<String, Any>? = null
)

data class TopicMessageRequest(
    @field:NotBlank(message = "Topic cannot be blank")
    @JsonProperty("topic")
    val topic: String,
    
    @field:NotBlank(message = "Message cannot be blank")
    @JsonProperty("message")
    val message: String,
    
    @JsonProperty("key")
    val key: String? = null,
    
    @JsonProperty("headers")
    val headers: Map<String, String>? = null
)

// Kafka Message Models
data class KafkaMessage(
    @JsonProperty("id")
    val id: String,
    
    @JsonProperty("payload")
    val payload: String,
    
    @JsonProperty("timestamp")
    val timestamp: Instant = Instant.now(),
    
    @JsonProperty("correlationId")
    val correlationId: String? = null,
    
    @JsonProperty("headers")
    val headers: Map<String, String>? = null,
    
    @JsonProperty("source")
    val source: String? = null,
    
    @JsonProperty("version")
    val version: String = "1.0"
)

data class UserEvent(
    @JsonProperty("userId")
    val userId: String,
    
    @JsonProperty("eventType")
    val eventType: EventType,
    
    @JsonProperty("data")
    val data: Map<String, Any>,
    
    @JsonProperty("timestamp")
    val timestamp: Instant = Instant.now(),
    
    @JsonProperty("sessionId")
    val sessionId: String? = null,
    
    @JsonProperty("correlationId")
    val correlationId: String? = null
)

data class TransactionEvent(
    @JsonProperty("transactionId")
    val transactionId: String,
    
    @JsonProperty("userId")
    val userId: String,
    
    @JsonProperty("eventType")
    val eventType: TransactionEventType,
    
    @JsonProperty("amount")
    val amount: String,
    
    @JsonProperty("currency")
    val currency: String,
    
    @JsonProperty("status")
    val status: TransactionStatus,
    
    @JsonProperty("timestamp")
    val timestamp: Instant = Instant.now(),
    
    @JsonProperty("metadata")
    val metadata: Map<String, Any>? = null
)

// Enums
enum class EventType {
    USER_CREATED,
    USER_UPDATED,
    USER_DELETED,
    USER_ACTION,
    MESSAGE_SENT,
    SESSION_STARTED,
    SESSION_ENDED,
    LOGIN_ATTEMPT,
    LOGOUT
}

enum class TransactionEventType {
    TRANSACTION_CREATED,
    TRANSACTION_PROCESSED,
    TRANSACTION_COMPLETED,
    TRANSACTION_FAILED,
    TRANSACTION_CANCELLED
}

enum class TransactionStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED,
    CANCELLED,
    EXPIRED
}

// Response DTOs
data class MessageResponse(
    @JsonProperty("success")
    val success: Boolean,
    
    @JsonProperty("messageId")
    val messageId: String? = null,
    
    @JsonProperty("error")
    val error: String? = null,
    
    @JsonProperty("timestamp")
    val timestamp: Instant = Instant.now(),
    
    @JsonProperty("metadata")
    val metadata: Map<String, Any>? = null
)

data class AsyncMessageResponse(
    @JsonProperty("acknowledged")
    val acknowledged: Boolean,
    
    @JsonProperty("partition")
    val partition: Int? = null,
    
    @JsonProperty("offset")
    val offset: Long? = null,
    
    @JsonProperty("timestamp")
    val timestamp: Long? = null,
    
    @JsonProperty("error")
    val error: String? = null,
    
    @JsonProperty("messageId")
    val messageId: String? = null
)

data class BatchResponse(
    @JsonProperty("totalSent")
    val totalSent: Int,
    
    @JsonProperty("successCount")
    val successCount: Int,
    
    @JsonProperty("failureCount")
    val failureCount: Int,
    
    @JsonProperty("results")
    val results: List<MessageResponse>,
    
    @JsonProperty("timestamp")
    val timestamp: Instant = Instant.now()
)

// Monitoring DTOs
data class ConsumerStatistics(
    @JsonProperty("totalProcessed")
    val totalProcessed: Long,
    
    @JsonProperty("totalErrors")
    val totalErrors: Long,
    
    @JsonProperty("errorRate")
    val errorRate: Double,
    
    @JsonProperty("averageProcessingTimeMs")
    val averageProcessingTimeMs: Double,
    
    @JsonProperty("lastProcessedTimestamp")
    val lastProcessedTimestamp: Instant?,
    
    @JsonProperty("uptime")
    val uptime: String,
    
    @JsonProperty("currentLag")
    val currentLag: Long? = null,
    
    @JsonProperty("topicsConsumed")
    val topicsConsumed: List<String>
)

data class ProcessedMessage(
    @JsonProperty("messageId")
    val messageId: String,
    
    @JsonProperty("originalPayload")
    val originalPayload: String,
    
    @JsonProperty("processedAt")
    val processedAt: Instant,
    
    @JsonProperty("processingTimeMs")
    val processingTimeMs: Long,
    
    @JsonProperty("topic")
    val topic: String,
    
    @JsonProperty("partition")
    val partition: Int,
    
    @JsonProperty("offset")
    val offset: Long,
    
    @JsonProperty("successful")
    val successful: Boolean,
    
    @JsonProperty("errorMessage")
    val errorMessage: String? = null
)

// Health Check DTOs
data class HealthResponse(
    @JsonProperty("status")
    val status: HealthStatus,
    
    @JsonProperty("service")
    val service: String,
    
    @JsonProperty("timestamp")
    val timestamp: Instant = Instant.now(),
    
    @JsonProperty("version")
    val version: String,
    
    @JsonProperty("dependencies")
    val dependencies: Map<String, HealthStatus>? = null,
    
    @JsonProperty("details")
    val details: Map<String, Any>? = null
)

enum class HealthStatus {
    UP, DOWN, DEGRADED, UNKNOWN
}