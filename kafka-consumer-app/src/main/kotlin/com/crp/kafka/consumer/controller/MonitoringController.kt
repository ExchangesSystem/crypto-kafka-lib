package com.crp.kafka.consumer.controller

import com.crp.kafka.consumer.service.MessageProcessorService
import com.crp.kafka.shared.dto.ConsumerStatistics
import com.crp.kafka.shared.dto.HealthResponse
import com.crp.kafka.shared.dto.HealthStatus
import com.crp.kafka.shared.dto.ProcessedMessage
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/monitoring")
class MonitoringController(
    @Autowired
    private val messageProcessorService: MessageProcessorService
) {
    
    /**
     * Get consumer statistics
     */
    @GetMapping("/stats")
    fun getStatistics(): ResponseEntity<ConsumerStatistics> {
        return ResponseEntity.ok(messageProcessorService.getStatistics())
    }
    
    /**
     * Get processed message by ID
     */
    @GetMapping("/message/{messageId}")
    fun getProcessedMessage(@PathVariable messageId: String): ResponseEntity<ProcessedMessage> {
        val message = messageProcessorService.getProcessedMessage(messageId)
        return if (message != null) {
            ResponseEntity.ok(message)
        } else {
            ResponseEntity.notFound().build()
        }
    }
    
    /**
     * Get topic-specific statistics
     */
    @GetMapping("/topics/stats")
    fun getTopicStatistics(): ResponseEntity<Map<String, Long>> {
        return ResponseEntity.ok(messageProcessorService.getTopicStatistics())
    }
    
    /**
     * Clear processed message cache
     */
    @PostMapping("/cache/clear")
    fun clearCache(): ResponseEntity<Map<String, String>> {
        messageProcessorService.clearCache()
        return ResponseEntity.ok(mapOf(
            "status" to "success",
            "message" to "Cache cleared successfully"
        ))
    }
    
    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    fun health(): ResponseEntity<HealthResponse> {
        val statistics = messageProcessorService.getStatistics()
        val isHealthy = statistics.errorRate < 10.0 // Less than 10% error rate is considered healthy
        
        return ResponseEntity.ok(HealthResponse(
            status = if (isHealthy) HealthStatus.UP else HealthStatus.DEGRADED,
            service = "kafka-consumer",
            version = "1.0.0",
            details = mapOf(
                "totalProcessed" to statistics.totalProcessed,
                "totalErrors" to statistics.totalErrors,
                "errorRate" to "${statistics.errorRate}%",
                "uptime" to statistics.uptime,
                "topicsConsumed" to statistics.topicsConsumed.size
            )
        ))
    }
    
    /**
     * Get detailed health information
     */
    @GetMapping("/health/detailed")
    fun detailedHealth(): ResponseEntity<Map<String, Any>> {
        val statistics = messageProcessorService.getStatistics()
        val topicStats = messageProcessorService.getTopicStatistics()
        
        return ResponseEntity.ok(mapOf(
            "service" to "kafka-consumer-app",
            "version" to "1.0.0",
            "status" to if (statistics.errorRate < 10.0) "UP" else "DEGRADED",
            "statistics" to statistics,
            "topicStatistics" to topicStats,
            "recommendations" to getHealthRecommendations(statistics)
        ))
    }
    
    private fun getHealthRecommendations(stats: ConsumerStatistics): List<String> {
        val recommendations = mutableListOf<String>()
        
        if (stats.errorRate > 5.0) {
            recommendations.add("High error rate detected. Check consumer logs and message formats.")
        }
        
        if (stats.averageProcessingTimeMs > 1000) {
            recommendations.add("High average processing time. Consider optimizing message processing logic.")
        }
        
        if (stats.currentLag != null && stats.currentLag > 10000) {
            recommendations.add("High consumer lag detected. Consider scaling up consumers.")
        }
        
        if (recommendations.isEmpty()) {
            recommendations.add("Consumer is operating normally.")
        }
        
        return recommendations
    }
}