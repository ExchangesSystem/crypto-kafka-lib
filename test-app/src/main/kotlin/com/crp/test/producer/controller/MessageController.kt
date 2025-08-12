package com.crp.test.producer.controller

import com.crp.test.dto.*
import com.crp.test.producer.service.MessageProducerService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/messages")
class MessageController(
    @Autowired
    private val messageProducerService: MessageProducerService
) {
    
    // Simple fire-and-forget message
    @PostMapping("/send")
    fun sendMessage(@RequestBody request: SimpleMessageRequest): ResponseEntity<MessageResponse> {
        return try {
            val messageId = messageProducerService.sendSimpleMessage(request.message)
            ResponseEntity.ok(MessageResponse(
                success = true,
                messageId = messageId
            ))
        } catch (e: Exception) {
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(MessageResponse(
                    success = false,
                    error = e.message
                ))
        }
    }
    
    // Send message and wait for acknowledgment
    @PostMapping("/send-with-ack")
    fun sendMessageWithAck(@RequestBody request: SimpleMessageRequest): Mono<ResponseEntity<AsyncMessageResponse>> {
        return messageProducerService.sendMessageWithResult(request.message)
            .map { result ->
                ResponseEntity.ok(AsyncMessageResponse(
                    acknowledged = true,
                    partition = result.recordMetadata?.partition(),
                    offset = result.recordMetadata?.offset(),
                    timestamp = result.recordMetadata?.timestamp()
                ))
            }
            .onErrorResume { error ->
                Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(AsyncMessageResponse(
                        acknowledged = false,
                        error = error.message
                    )))
            }
    }
    
    // Send user event
    @PostMapping("/user-event")
    fun sendUserEvent(@RequestBody request: UserMessageRequest): ResponseEntity<MessageResponse> {
        return try {
            val messageId = messageProducerService.sendUserEvent(
                userId = request.userId,
                message = request.message,
                metadata = request.metadata ?: emptyMap()
            )
            ResponseEntity.ok(MessageResponse(
                success = true,
                messageId = messageId
            ))
        } catch (e: Exception) {
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(MessageResponse(
                    success = false,
                    error = e.message
                ))
        }
    }
    
    // Send message to specific topic
    @PostMapping("/send-to-topic")
    fun sendToTopic(
        @RequestParam topic: String,
        @RequestBody request: SimpleMessageRequest
    ): ResponseEntity<MessageResponse> {
        return try {
            val messageId = messageProducerService.sendToTopic(topic, request.message)
            ResponseEntity.ok(MessageResponse(
                success = true,
                messageId = messageId
            ))
        } catch (e: Exception) {
            ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(MessageResponse(
                    success = false,
                    error = e.message
                ))
        }
    }
    
    // Batch send messages
    @PostMapping("/batch-send")
    fun batchSend(@RequestBody messages: List<SimpleMessageRequest>): ResponseEntity<List<MessageResponse>> {
        val responses = messages.map { request ->
            try {
                val messageId = messageProducerService.sendSimpleMessage(request.message)
                MessageResponse(success = true, messageId = messageId)
            } catch (e: Exception) {
                MessageResponse(success = false, error = e.message)
            }
        }
        return ResponseEntity.ok(responses)
    }
    
    // Health check
    @GetMapping("/health")
    fun health(): ResponseEntity<Map<String, String>> {
        return ResponseEntity.ok(mapOf(
            "status" to "UP",
            "service" to "kafka-producer"
        ))
    }
}