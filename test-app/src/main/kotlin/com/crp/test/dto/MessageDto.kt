package com.crp.test.dto

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

// Request DTOs
data class SimpleMessageRequest(
    @JsonProperty("message")
    val message: String
)

data class UserMessageRequest(
    @JsonProperty("userId")
    val userId: String,
    @JsonProperty("message")
    val message: String,
    @JsonProperty("metadata")
    val metadata: Map<String, Any>? = null
)

// Kafka Message Models
data class KafkaMessage(
    val id: String,
    val payload: String,
    val timestamp: Instant = Instant.now(),
    val correlationId: String? = null,
    val headers: Map<String, String>? = null
)

data class UserEvent(
    val userId: String,
    val eventType: EventType,
    val data: Map<String, Any>,
    val timestamp: Instant = Instant.now()
)

enum class EventType {
    USER_CREATED,
    USER_UPDATED,
    USER_DELETED,
    USER_ACTION,
    MESSAGE_SENT
}

// Response DTOs
data class MessageResponse(
    val success: Boolean,
    val messageId: String? = null,
    val error: String? = null,
    val timestamp: Instant = Instant.now()
)

data class AsyncMessageResponse(
    val acknowledged: Boolean,
    val partition: Int? = null,
    val offset: Long? = null,
    val timestamp: Long? = null,
    val error: String? = null
)