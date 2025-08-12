package com.crp.test.consumer.controller

import com.crp.test.consumer.service.MessageProcessorService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/monitoring")
class MonitoringController(
    @Autowired
    private val messageProcessorService: MessageProcessorService
) {
    
    @GetMapping("/stats")
    fun getStatistics(): ResponseEntity<Map<String, Any>> {
        return ResponseEntity.ok(messageProcessorService.getStatistics())
    }
    
    @GetMapping("/message/{messageId}")
    fun getProcessedMessage(@PathVariable messageId: String): ResponseEntity<Any> {
        val message = messageProcessorService.getProcessedMessage(messageId)
        return if (message != null) {
            ResponseEntity.ok(message)
        } else {
            ResponseEntity.notFound().build()
        }
    }
    
    @PostMapping("/cache/clear")
    fun clearCache(): ResponseEntity<Map<String, String>> {
        messageProcessorService.clearCache()
        return ResponseEntity.ok(mapOf("status" to "Cache cleared"))
    }
    
    @GetMapping("/health")
    fun health(): ResponseEntity<Map<String, String>> {
        return ResponseEntity.ok(mapOf(
            "status" to "UP",
            "service" to "kafka-consumer"
        ))
    }
}